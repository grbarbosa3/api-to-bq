# src/load.py
from google.cloud import bigquery
import pandas as pd

def ensure_table_ingestion_partitioned(client: bigquery.Client, project: str, dataset: str, table: str, df: pd.DataFrame):
    table_ref = f"{project}.{dataset}.{table}"
    try:
        client.get_table(table_ref)  # já existe
        return
    except Exception:
        # schema básico inferido do DF (fallback). Se quiser, defina explicitamente.
        job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        # cria uma tabela vazia com schema via DF e, em seguida, ajusta particionamento
        tmp = df.head(1)  # 1 linha só para inferir schema
        job = client.load_table_from_dataframe(tmp, table_ref, job_config=job_cfg)
        job.result()

        tbl = client.get_table(table_ref)
        tbl.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY  # ingestion-time partition
        )
        client.update_table(tbl, ["time_partitioning"])

def upsert_merge_bq(df: pd.DataFrame, project: str, dataset: str, table: str, key_cols: list):
    """
    SANDBOX-friendly: sobrescreve a tabela inteira (WRITE_TRUNCATE) sem DML,
    com particionamento por ingestion time.
    """
    client = bigquery.Client(project=project)
    ensure_table_ingestion_partitioned(client, project, dataset, table, df)

    table_ref = f"{project}.{dataset}.{table}"
    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_cfg)
    job.result()

