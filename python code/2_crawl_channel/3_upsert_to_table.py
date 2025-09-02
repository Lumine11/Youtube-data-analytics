import sys
from pathlib import Path
# Project root = 1 cấp trên file hiện tại
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import env_utils
import gcp_io


#MERGE staging và video_basic info table dựa trên video id, giữ lại thông tin mới nhất
from typing import List, Iterable, Optional
from google.cloud import bigquery


def main() -> None:
    cfg = env_utils.load_env()

    # query = f"""
    # SELECT column_name
    # FROM `{cfg.project_id}.{cfg.clean_dataset}.INFORMATION_SCHEMA.COLUMNS`
    # WHERE table_name = '{cfg.channel_info_table}';
    # """

    # result = gcp_io.execute_sql(
    #     project_id=cfg.project_id,
    #     query=query
    # )

    # columns = [row['column_name'] for row in result]
    # print(columns)

    all_cols = ['id', 'title', 'description', 'publishedAt', 'country', 'defaultLanguage', 'viewCount', 'subscriberCount', 'videoCount', 'uploadsPlaylistId', 'topicCategories', 'crawl_date']
    update_cols =['viewCount', 'subscriberCount', 
           'videoCount', 'uploadsPlaylistId', 'crawl_date']
    

    gcp_io.merge_tables(
        project_id=cfg.project_id,
        src_dataset=cfg.staging_dataset,
        target_dataset=cfg.clean_dataset,
        target_table=cfg.channel_info_table,
        staging_table=cfg.channel_staging_table,
        updated_cols=update_cols,
        all_cols=all_cols
    )

if __name__ == "__main__":
    main()