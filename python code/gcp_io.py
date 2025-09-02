from __future__ import annotations

import os
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass

import pandas as pd
from google.cloud import storage, bigquery



# ----------------------------
# ENV / CLIENTS
# ----------------------------
def get_storage_client(project_id: Optional[str] = None) -> storage.Client:
    return storage.Client(project=project_id) if project_id else storage.Client()

def get_bq_client(project_id: Optional[str] = None, location: Optional[str] = None) -> bigquery.Client:
    return bigquery.Client(project=project_id, location=location) if (project_id or location) else bigquery.Client()

def ensure_dataset(client: bigquery.Client, dataset_id: str, location: Optional[str] = None) -> None:
    ds_ref = f"{client.project}.{dataset_id}"
    ds = bigquery.Dataset(ds_ref)
    if location:
        ds.location = location
    client.create_dataset(ds, exists_ok=True)


# ----------------------------
# GCS HELPERS
# ----------------------------
def list_gcs_uris(bucket: str, prefix: str, project_id: Optional[str] = None) -> List[str]:
    cli = get_storage_client(project_id)
    bkt = cli.bucket(bucket)
    return [f"gs://{bucket}/{b.name}" for b in bkt.list_blobs(prefix=prefix)]

def read_latest_json_from_gcs(bucket: str, prefix: str, project_id: Optional[str] = None, today_only: bool = False) -> pd.DataFrame:
    cli = get_storage_client(project_id)
    bkt = cli.bucket(bucket)
    
    blobs = list(bkt.list_blobs(prefix=prefix))
    if not blobs:
        raise FileNotFoundError(f"No blobs under gs://{bucket}/{prefix}")
    if today_only:
        from datetime import date
        blobs = [b for b in blobs if b.time_created.date() == date.today()]
        if not blobs:
            raise FileNotFoundError("No blobs created today.")
    blob = max(blobs, key=lambda b: b.time_created)
    txt = blob.download_as_text(encoding="utf-8")
    data = json.loads(txt)
    return pd.DataFrame(data)

def upload_json_to_gcs(bucket: str, path: str, data: Any, project_id: Optional[str] = None, content_type: str = "application/json; charset=utf-8") -> str:
    cli = get_storage_client(project_id)
    bkt = cli.bucket(bucket)
    blob = bkt.blob(path)
    payload = json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    blob.upload_from_string(payload, content_type=content_type)
    return f"gs://{bucket}/{path}"


# ----------------------------
# BIGQUERY HELPERS
# ----------------------------
@dataclass
class BQTarget:
    project_id: str
    dataset: str
    table: str
    location: str = "asia-southeast1"

    @property
    def fqtn(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}"

def write_df_to_bq(
    df: pd.DataFrame,
    target: BQTarget,
    write_mode: str,
    autodetect: bool = True,
    schema: Optional[List[bigquery.SchemaField]] = None,
) -> None:
    """
    Ghi một DataFrame vào BigQuery table (theo target).
    Mặc định: append, autodetect schema.
    """
    client = bigquery.Client(project=target.project_id, location=target.location)
    ensure_dataset(client, target.dataset, target.location)

    # Map write_mode string -> BigQuery WriteDisposition
    if write_mode == "append":
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif write_mode == "overwrite":
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif write_mode == "empty":
        write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
    else:
        raise ValueError(f"❌ Invalid write_mode: {write_mode}. Must be 'append', 'overwrite', or 'empty'.")



    job_cfg = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=autodetect,
    )
    if schema:
        job_cfg.schema = schema
        job_cfg.autodetect = False

    job = client.load_table_from_dataframe(df, target.fqtn, job_config=job_cfg)
    job.result()
    print(f"✅ Data uploaded to BigQuery: {target.fqtn}")


# def merge_tables(project_id: str, src_dataset: str, target_dataset: str ,target_table: str, staging_table: str, cols: list):

#     """Merge table based on column id"""
#     client = bigquery.Client(project=project_id)

#     update_set = ",\n  ".join([f"T.{c} = S.{c}" for c in cols])
#     query = f"""
#     MERGE INTO `{project_id}.{target_dataset}.{target_table}` T
#     USING `{project_id}.{src_dataset}.{staging_table}` S
#     ON T.id = S.id
#     WHEN MATCHED THEN
#     UPDATE SET
#         {update_set}
#     WHEN NOT MATCHED THEN
#     INSERT ({",".join(cols)})
#     VALUES ({",".join(["S."+c for c in cols])})
#     """
#     print(query)

#     job = client.query(query)
#     job.result()
#     print(f"✅ Merge thành công vào bảng {target_table}")


from google.cloud import bigquery

def merge_tables(
    project_id: str,
    src_dataset: str,
    target_dataset: str,
    target_table: str,
    staging_table: str,
    updated_cols: list,   # chỉ update khi MATCHED
    all_cols: list        # toàn bộ cột để INSERT khi NOT MATCHED
) -> None:
    """MERGE từ staging vào target dựa trên khóa id.
    - updated_cols: các cột sẽ được set trong WHEN MATCHED THEN UPDATE
    - all_cols:     toàn bộ cột dùng cho INSERT khi NOT MATCHED
    """

    client = bigquery.Client(project=project_id)

    # Bảo vệ: loại bỏ 'id' khỏi danh sách update (nếu có)
    cleaned_update_cols = [c for c in updated_cols if c.lower() != "id"]

    # Kiểm tra tối thiểu
    if not all_cols:
        raise ValueError("all_cols không được rỗng.")
    if "id" not in [c.lower() for c in all_cols]:
        raise ValueError("all_cols phải chứa khóa 'id'.")

    # Phần SET cho WHEN MATCHED (chỉ khi có cột để update)
    update_set_sql = ",\n  ".join([f"T.{c} = S.{c}" for c in cleaned_update_cols]) if cleaned_update_cols else ""

    # Phần INSERT cho WHEN NOT MATCHED
    insert_cols_sql = ",".join(all_cols)
    insert_vals_sql = ",".join([f"S.{c}" for c in all_cols])

    # Ghép câu lệnh MERGE (bỏ hẳn WHEN MATCHED nếu không có cột để update)
    matched_clause = (
        f"""
    WHEN MATCHED THEN
      UPDATE SET
        {update_set_sql}
        """ if update_set_sql else ""
    )

    query = f"""
    MERGE `{project_id}.{target_dataset}.{target_table}` T
    USING `{project_id}.{src_dataset}.{staging_table}` S
    ON T.id = S.id
    {matched_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols_sql})
      VALUES ({insert_vals_sql})
    """

    print(query.strip())
    job = client.query(query)
    job.result()
    print(f"✅ Merge thành công vào bảng {target_table}")


def execute_sql(project_id: str, query: str) -> None:
    client = bigquery.Client(project=project_id)
    job = client.query(query)
    print(f"✅ Query executed successfully: {query}")
    return job.result()