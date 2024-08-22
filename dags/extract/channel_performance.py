from airflow.hooks.base_hook import BaseHook
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import json


def extract_channel_performance():
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

    df = pd.read_csv("/opt/airflow/dags/data_source/channel_performances.csv")

    df = df.dropna(subset=["channel_id"])
    # Konversi kolom
    df["channel_id"] = df["channel_id"].astype("Int64", errors="ignore")
    df["transaction_date"] = pd.to_datetime(df["date"], errors="coerce")
    df["total_transactions"] = df["total_transactions"].astype("Int64", errors="ignore")
    df["total_clicks"] = df["total_clicks"].round().astype("Int64", errors="ignore")
    df["total_impressions"] = (
        df["total_impressions"].round().astype("Int64", errors="ignore")
    )
    df = df.drop("date", axis=1)
    print(df.dtypes)
    print(df.columns)
    print(df)

    dataset_id = "ecommers_de4_team_2"
    table_id = "raw_channel_performances"

    table_ref = client.dataset(dataset_id).table(table_id)

    # Load data ke BigQuery
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait for the job to complete
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
