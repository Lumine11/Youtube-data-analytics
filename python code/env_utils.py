from __future__ import annotations

import os
from typing import Optional
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# Lấy thư mục cha 1 cấp so với file hiện tại
ROOT = Path(__file__).resolve().parents[1]

# Ghép thêm tên file .env
path = ROOT / ".env"

@dataclass
class EnvConfig:
    api: str           # YouTube API key
    project_id: str    # GCP project ID

    #GCS
    bucket_name: str   # GCS bucket name
    search_result_raw: str   # GCS path to raw search results
    detailed_video_info: str   # GCS path to raw video info
    channel_raw_info: str      # GCS path to raw channel info
    video_captions: str        # GCS path to video captions


    #BigQuery
    clean_dataset: str              # BigQuery dataset
    video_info_table: str      # BigQuery table to video info table
    channel_info_table: str    # BigQuery table to channel info table
    video_captions_table: str

    staging_dataset: str         # BigQuery staging dataset
    video_staging_table: str     # BigQuery video staging table
    channel_staging_table: str   # BigQuery channel staging table


    #Credentials
    credentials: str   # Path to service account JSON


def load_env(env_file: Optional[Path] = path) -> EnvConfig:
    """
    Load environment variables từ .env và trả về EnvConfig.
    Expect: API_KEY_YTB, PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS, ...
    """
    if env_file is None:
        ROOT = Path(__file__).resolve().parents[2]
        env_file = ROOT / ".env"

    load_dotenv(dotenv_path=env_file)

    api = os.getenv("API_KEY_YTB")
    project_id = os.getenv("PROJECT_ID")
    credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not api:
        raise EnvironmentError("Missing API_KEY_YTB in .env (YouTube API key)")
    if not project_id or not credentials:
        raise EnvironmentError("Missing PROJECT_ID or GOOGLE_APPLICATION_CREDENTIALS in .env")

    # export để google.cloud nhận
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials

    return EnvConfig(
        api=api,
        project_id=project_id,
        bucket_name=os.getenv("BUCKET_NAME", ""),
        search_result_raw=os.getenv("SEARCH_RESULT_RAW", ""),
        detailed_video_info=os.getenv("DETAILED_VIDEO_INFO", ""),
        channel_raw_info=os.getenv("CHANNEL_RAW_INFO", ""),
        video_captions=os.getenv("VIDEO_CAPTIONS", ""),

        clean_dataset=os.getenv("DATASET", ""),
        video_info_table=os.getenv("VIDEO_INFO_TABLE", ""),
        channel_info_table=os.getenv("CHANNEL_INFO_TABLE", ""),
        video_captions_table=os.getenv("VIDEO_CAPTIONS_TABLE", ""),


        staging_dataset=os.getenv("STAGING_DATASET", ""),
        video_staging_table=os.getenv("VIDEO_STAGING_TABLE", ""),
        channel_staging_table=os.getenv("CHANNEL_STAGING_TABLE", ""),


        credentials=credentials,
    )
