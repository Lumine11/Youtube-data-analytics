import json
import pandas as pd
from langdetect import detect, DetectorFactory
from typing import Any
import isodate as i

DetectorFactory.seed = 0

import sys
from pathlib import Path
# Project root = 1 cấp trên file hiện tại
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import env_utils
import gcp_io


# ---------- PREPROCESS ----------
def json_parse_safe(val: Any) -> Any:
    """Parse chuỗi JSON về dict an toàn; nếu đã là dict/list thì trả về nguyên."""
    if isinstance(val, (dict, list)) or pd.isna(val):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return val
    return val


def split_json_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Chuẩn hoá cột JSON (snippet/contentDetails/statistics) thành các cột phẳng.
    Không dùng eval; parse an toàn.
    """
    if column not in df.columns:
        return df
    df = df.copy()
    df[column] = df[column].apply(json_parse_safe)
    norm = pd.json_normalize(df[column])
    # norm.columns = [f"{column}.{c}" for c in norm.columns]
    return pd.concat([df.drop(columns=[column]), norm], axis=1)


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tiền xử lý: phẳng JSON, đổi kiểu dữ liệu, loại cột rác, chuẩn hoá danh sách, drop NA,
    ánh xạ category, thêm crawl_date (hôm nay).
    """
    # 1) Phẳng các cột lớn
    for col in ["snippet", "contentDetails", "statistics"]:
        df = split_json_column(df, col)

    # print(df.columns)

    # 2) Loại cột không cần thiết
    drop_columns = [
        'etag', 'kind',
        'thumbnails.medium.url', 'thumbnails.medium.width', 'thumbnails.medium.height',
        'thumbnails.high.url', 'thumbnails.high.width', 'thumbnails.high.height',
        'thumbnails.standard.url', 'thumbnails.standard.width', 'thumbnails.standard.height', 'thumbnails.maxres.url',
        'thumbnails.maxres.width', 'thumbnails.maxres.height', 'thumbnails.default.width', 'thumbnails.default.height',
        'localized.title', 'localized.description','thumbnails.default.url' ,
        'projection', 'liveBroadcastContent', 'dimension', 'definition', 'projection'
        'favoriteCount'
    ]
    df.drop(columns=drop_columns, inplace=True, errors="ignore")

    # 3) Kiểu dữ liệu
    df["publishedAt"] = pd.to_datetime(df.get("publishedAt"), errors="coerce")

    if "caption" in df.columns:
        df["caption"] = df["caption"].apply(lambda x: True if str(x).lower() == "true" else False)

    for c in ["viewCount", "likeCount", "commentCount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "duration" in df.columns:
        df["duration_minutes"] = df["duration"].apply(
            lambda x: round(i.parse_duration(x).total_seconds() / 60, 3) if isinstance(x, str) else None
        )
        df.drop(columns=["duration"], inplace=True, errors="ignore")

    # 4) Bỏ dòng không hợp lệ
    if "duration_minutes" in df.columns:
        df.dropna(subset=["duration_minutes"], inplace=True)
        df = df[df["duration_minutes"] > 0]

    # 5) Điền NA số & cast
    for c in ["likeCount", "commentCount", "viewCount"]:
        if c in df.columns:
            df[c] = df[c].fillna(0).astype(int)

    # 6) Ánh xạ category
    category_mapping = {
        "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music",
        "15": "Pets & Animals", "17": "Sports", "19": "Travel & Events",
        "20": "Gaming", "22": "People & Blogs", "23": "Comedy",
        "24": "Entertainment", "25": "News & Politics", "26": "Howto & Style",
        "27": "Education", "28": "Science & Technology", "29": "Nonprofits & Activism",
    }
    if "categoryId" in df.columns:
        df["categoryName"] = df["categoryId"].astype(str).map(category_mapping)

    # 7) Detect ngôn ngữ fallback (chỉ khi thiếu defaultLanguage)
    def detect_language_safe(text):
        try:
            return detect(str(text))
        except Exception:
            return "unknown"

    if "defaultLanguage" in df.columns:
        df["defaultLanguage"] = df["defaultLanguage"].fillna(
            df.get("description", pd.Series([None] * len(df))).apply(detect_language_safe)
        )

    # 8) Danh sách → chuỗi (để nạp BQ ổn định)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: "; ".join(x) if isinstance(x, list) else x)

    # 9) Thêm crawl_date = hôm nay (UTC date)
    df["crawl_date"] = pd.to_datetime(pd.Timestamp.utcnow().date())

    # 10) Chuẩn khoá video id
    #   - cột id là videoId ở schema item; giữ tên 'id' cho BQ table
    if "id" not in df.columns and "videoId" in df.columns:
        df.rename(columns={"videoId": "id"}, inplace=True)

    # Drop trùng ID (giữ bản đầu)
    if "id" in df.columns:
        df.drop_duplicates(subset=["id"], keep="first", inplace=True)
        df.reset_index(drop=True, inplace=True)

    return df

def main() -> None:
    cfg = env_utils.load_env()

    # 1) Đọc toàn bộ file video-info TẠO HÔM NAY
    df_raw = gcp_io.read_latest_json_from_gcs(
        bucket=cfg.bucket_name,
        prefix=cfg.detailed_video_info,
        project_id=cfg.project_id
    )

    # 2) Tiền xử lý
    df_clean = preprocess(df_raw)

    # 3) Nạp BigQuery
    print(df_clean)
    # load_dataframe_to_staging(df_clean, project_id, BQ_DATASET, BQ_TABLE, BQ_LOCATION)
    gcp_io.write_df_to_bq(
        df_clean,
        gcp_io.BQTarget(
            project_id=cfg.project_id,
            dataset=cfg.staging_dataset,
            table=cfg.video_staging_table
            # location=cfg.bq_location
        ),
        write_mode='overwrite',
        autodetect=True,
    )


if __name__ == "__main__":
    main()
