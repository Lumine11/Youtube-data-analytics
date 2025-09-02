# Project root = 1 cấp trên file hiện tại

import sys
from zoneinfo import ZoneInfo
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
import env_utils
import gcp_io
from datetime import datetime
from googleapiclient.discovery import build


TZ = ZoneInfo("Asia/Ho_Chi_Minh")

import time
from pathlib import Path
from typing import List, Dict, Any


def crawl_channel_info(
    youtube,
    channel_list: List[str],
    batch_size: int = 50, #cố định, mỗi request tối đa 50 channel
    sleep_time: float = 0.1
) -> List[Dict[str, Any]]:
    """
    Crawl snippet, statistics, contentDetails, topicDetails của các channel trong channel_list.
    Lưu dữ liệu raw vào file json.

    Args:
        youtube: YouTube API service object (tạo bằng googleapiclient.discovery.build).
        channel_list (List[str]): Danh sách channel IDs.
        output_dir (str): Thư mục lưu file json.
        output_filename (str): Tên file json đầu ra (vd: "channel_raw_info20250902.json").
        batch_size (int): Số channel tối đa cho mỗi request (mặc định 50).
        sleep_time (float): Thời gian nghỉ giữa các request để tránh quota exceeded.

    Returns:
        List[Dict[str, Any]]: Danh sách dữ liệu raw đã crawl được.
    """
    raw_data = []

    for i in range(0, len(channel_list), batch_size):
        batch = channel_list[i:i + batch_size]
        id_string = ",".join(batch)

        try:
            response = youtube.channels().list(
                part="snippet,statistics,contentDetails,topicDetails",
                id=id_string
            ).execute()

            raw_data.extend(response.get("items", []))
            print(f"✅ Crawled {i} → {i + len(batch) - 1}")

            time.sleep(sleep_time)  # hạn chế quota exceeded

        except Exception as e:
            print(f"❌ Error with batch {i}-{i + len(batch) - 1}: {e}")
            continue

    return raw_data


def main() -> None:
    cfg = env_utils.load_env()
    ts = datetime.now(TZ).strftime("_%Y%m%d_%H%M%S")


    # 1) lấy channel id từ bảng tạm
    query = f"""
    SELECT distinct channelId
    FROM `{cfg.project_id}.{cfg.staging_dataset}.{cfg.video_staging_table}`
    """

    results = gcp_io.execute_sql(project_id=cfg.project_id, query=query)
    channel_list = [row.channelId for row in results]

    youtube = build("youtube", "v3", developerKey=cfg.api)

    channel_results = crawl_channel_info(youtube, channel_list)

    gcp_io.upload_json_to_gcs(
        project_id=cfg.project_id,
        data=channel_results,
        bucket=cfg.bucket_name,
        path=f"{cfg.channel_raw_info}channel_raw_info{ts}.json",
    )

if __name__ == "__main__":
    main()

