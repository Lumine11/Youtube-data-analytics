import sys
from zoneinfo import ZoneInfo
from pathlib import Path

from charset_normalizer import detect
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))
import env_utils
import gcp_io


TZ = ZoneInfo("Asia/Ho_Chi_Minh")
from pathlib import Path
import pandas as pd


#  Cell 7
def split_json_column(df, column):
    # Convert the JSON string to a dictionary
    df[column] = df[column].apply(lambda x: eval(x) if isinstance(x, str) else x)
    
    # Normalize the JSON column into separate columns
    json_df = pd.json_normalize(df[column])
    
    # Concatenate the new columns with the original DataFrame
    df = pd.concat([df.drop(columns=[column]), json_df], axis=1)
    
    return df

def detect_language(text):
    try:
        return detect(text)
    except:
        return 'unknown'
    
def preprocess(df):
    #remove duplicate base on id 
    df.drop_duplicates(subset=['id'], inplace=True)

    #  Cell 12
    #drop columns
    df.drop(columns=['etag', 'kind', 'thumbnails.default.url', 'thumbnails.default.width', 'thumbnails.default.height',
                        'thumbnails.medium.url', 'thumbnails.medium.width', 'thumbnails.medium.height',
                        'thumbnails.high.url', 'thumbnails.high.width', 'thumbnails.high.height',
                        'relatedPlaylists.likes', 'topicIds','customUrl', 'localized.title', 'localized.description', 'hiddenSubscriberCount'
                ], inplace=True, errors='ignore')


    #  Cell 15
    df['publishedAt'] = pd.to_datetime(df['publishedAt'],format = 'ISO8601')
    df['viewCount'] = pd.to_numeric(df['viewCount'], errors='coerce')
    df['subscriberCount'] = pd.to_numeric(df['subscriberCount'], errors='coerce')
    df['videoCount'] = pd.to_numeric(df['videoCount'], errors='coerce')


    #turn topicCategories into a string
    df['topicCategories'] = df['topicCategories'].apply(lambda x: '; '.join(x) if isinstance(x, list) else x)

    #  Cell 22
    #fill country, topicCategories with mode
    for column in ['country', 'topicCategories']:
        mode_value = df[column].mode()[0]
        df.fillna({column: mode_value}, inplace=True)

    #  Cell 23
    #fill null of default Language with Detect Language of description
    if 'defaultLanguage' not in df.columns:
        df['defaultLanguage'] = df['description'].apply(detect_language)
    else:
        df['defaultLanguage'] = df['defaultLanguage'].fillna(df['description'].apply(detect_language))

    #  Cell 24
    df['topicCategories'] = df['topicCategories'].str.replace('https://en.wikipedia.org/wiki/', '', regex=True)

    df.rename(columns={'relatedPlaylists.uploads': 'uploadsPlaylistId'}, inplace=True)

    df['crawl_date'] = pd.to_datetime('today').normalize()

    return df

def main():
    cfg = env_utils.load_env()

    df = gcp_io.read_latest_json_from_gcs(
        bucket = cfg.bucket_name,
        prefix = cfg.channel_raw_info,
        project_id=cfg.project_id
    )

    # print(df.columns)

    df = split_json_column(df, 'snippet')
    df = split_json_column(df, 'statistics')
    df = split_json_column(df, 'contentDetails')
    df = split_json_column(df, 'topicDetails')

    # print(df.columns)

    df = preprocess(df)

    # print(df.columns)

    gcp_io.write_df_to_bq(
        df=df,
        target=gcp_io.BQTarget(
            project_id=cfg.project_id,
            dataset=cfg.staging_dataset,
            table=cfg.channel_staging_table
            # location=cfg.bq_location
        ),
        write_mode='overwrite',
        autodetect=True
    )



if __name__ == "__main__":
    main()