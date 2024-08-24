from airflow.hooks.base_hook import BaseHook
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import json


def extract_acqusition_costs():
    # Koneksi ke BigQuery
    bq_conn_id = "bigquery_con"
    bq_conn = BaseHook.get_connection(bq_conn_id)
    credentials_info = json.loads(bq_conn.extra_dejson["keyfile_dict"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info
    )
    client = bigquery.Client(
        credentials=credentials,
        project=bq_conn.extra_dejson.get("project"),
    )

    df = pd.read_csv("/opt/airflow/dags/data_source/acquisition_cost.csv")
    df = df.dropna(subset=["channel_id"])

    # Konversi kolom
    df["channel_id"] = df["channel_id"].astype("Int64", errors="ignore")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    print(df.dtypes)
    print(df.columns)

    dataset_id = "ecommers_de4_team_2"
    table_id = "raw_acqusition_costs"

    table_ref = client.dataset(dataset_id).table(table_id)

    # Load data ke BigQuery
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
