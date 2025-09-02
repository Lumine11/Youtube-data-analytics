import sys
from pathlib import Path
# Project root = 1 cấp trên file hiện tại
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import env_utils
import gcp_io
import pandas as pd

from youtube_transcript_api import YouTubeTranscriptApi
import time
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound


def get_video_list(cfg: env_utils.EnvConfig):
    query = f"""
    (
    SELECT DISTINCT id
    FROM `{cfg.project_id}.{cfg.clean_dataset}.{cfg.video_info_table}`
    )
    EXCEPT DISTINCT
    (
    SELECT DISTINCT id
    FROM `{cfg.project_id}.{cfg.clean_dataset}.{cfg.video_captions_table}`
    )
    """
    print(query)
    results = gcp_io.execute_sql(project_id=cfg.project_id, query=query)

    # Lưu kết quả vào danh sách
    video_list = [row.id for row in results]
    return video_list

# #Cell 6
# video_list = video_list[:500]  # Giới hạn danh sách video để tránh quá tải

#Cell 7
def get_transcript_flexible(video_id):
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Ưu tiên phụ đề tiếng Anh
        try:
            return transcript_list.find_transcript(['en']).fetch(), 'en'
        except:
            pass

        # Nếu không có tiếng Anh, lấy bất kỳ cái nào fetch được
        for transcript in transcript_list:
            try:
                return transcript.fetch(), transcript.language_code
            except:
                continue

    except Exception as e:
        print(f"Không lấy được transcript cho video {video_id}: {e}")
        return None,None


def crawl_transcripts(video_list):
    data = []
    count = 0
    for v in video_list:
        only_text = ''
        transcript,lang = get_transcript_flexible(v)

        if transcript is None:
            print(f"No transcript found for video {v}")
            data.append({'id': v, 'transcript': 'No Transcript', 'lang': 'N/A'})
            count += 1
            continue

        for item in transcript:
            # only_text += item['text'] + ' '
            only_text += item.text + ' '
        
        data.append({'id': v, 'transcript': only_text, 'lang': lang})
        print(f"Video {count}/{len(video_list)}: {v}")
        count += 1

        time.sleep(3)        
        if count % 100 == 0:
            time.sleep(60)  # sleep for 1 minute every 100 videos to avoid hitting API limits

    return data


def main() -> None:
    cfg = env_utils.load_env()
    video_list = get_video_list(cfg)
    print(f"Total videos to process: {len(video_list)}")
    print(video_list)

    data = crawl_transcripts(video_list)
    print(f"Total transcripts crawled: {len(data)}")

    df = pd.DataFrame(data)
    if df.empty:
        print("⚠️ Không có dữ liệu để ghi.")
        return
    
    print(df)

    gcp_io.write_df_to_bq(
        df,
        gcp_io.BQTarget(
            project_id=cfg.project_id,
            dataset=cfg.clean_dataset,
            table=cfg.video_captions_table
        ),
        write_mode='append',
        autodetect=True,
    )


if __name__ == "__main__":
    main()
