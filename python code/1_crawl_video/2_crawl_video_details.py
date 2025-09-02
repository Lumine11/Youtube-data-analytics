
from __future__ import annotations

import os
import json
import time
from pathlib import Path
from typing import Any, List, Dict, Optional
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
import sys
from pathlib import Path
# Project root = 1 cấp trên file hiện tại
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
import env_utils
import pandas as pd
import gcp_io

SEARCH_URL_VIDEOS = "https://www.googleapis.com/youtube/v3/videos"
TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def clean_search_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reset index, loại trùng theo videoId.
    """
    if "videoId" not in df.columns:
        raise ValueError("DataFrame cần có cột 'videoId'.")
    df = df.copy()
    # df.reset_index(drop=True, inplace=True)
    df.drop_duplicates(subset=["videoId"], keep="first", inplace=True)
    # df.reset_index(drop=True, inplace=True)
    return df


def get_video_details(
    df: pd.DataFrame,
    start: int,
    end: int,
    api_key: str,
    request_pause: float = 0.1,
) -> List[Dict[str, Any]]:
    """
    Crawl chi tiết video (snippet, statistics, contentDetails) theo df['videoId'][start:end].
    Trả về list các item JSON từ YouTube Data API.
    """
    if "videoId" not in df.columns:
        raise ValueError("DataFrame cần có cột 'videoId'.")
    if not api_key:
        raise RuntimeError("Thiếu API_KEY_1.")

    video_ids = df["videoId"].iloc[start:end].dropna().astype(str).tolist()
    if not video_ids:
        return []

    all_results: List[Dict[str, Any]] = []
    session = requests.Session()

    for i in range(0, len(video_ids), 50):  # tối đa 50 ID mỗi request
        batch_ids = video_ids[i : i + 50]
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(batch_ids),
            "key": api_key,
        }
        resp = session.get(SEARCH_URL_VIDEOS, params=params, timeout=30)
        if resp.status_code == 200:
            payload = resp.json()
            all_results.extend(payload.get("items", []))
        else:
            print(f"❌ Lỗi batch {i}-{i+50}: {resp.status_code}")
            print(resp.text[:500])
        time.sleep(request_pause)

    return all_results

def main() -> None:
    cfg = env_utils.load_env()
    ts = datetime.now(TZ).strftime("_%Y%m%d_%H%M%S")


    # 1) Đọc file search-result hôm nay → df
    df = gcp_io.read_latest_json_from_gcs(
        bucket=cfg.bucket_name,
        prefix=cfg.search_result_raw,
        project_id=cfg.project_id,
    )

    # 2) Làm sạch (bỏ trùng videoId)
    df = clean_search_df(df)

    # 3) Lấy chi tiết cho một khoảng (ví dụ 0..100)
    results = get_video_details(df, start=0, end=len(df), api_key=cfg.api)

    # 4) Lưu chi tiết lên GCS (1 file/timestamp)
    gcp_io.upload_json_to_gcs(
        bucket=cfg.bucket_name,
        data=results,
        path=f"{cfg.detailed_video_info}video_info_{ts}.json",
        project_id=cfg.project_id,
    )


if __name__ == "__main__":
    main()
