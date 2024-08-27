from google.oauth2 import service_account
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from google.cloud import bigquery
import pandas as pd
import json


def extract_products():
    # Ambil koneksi PostgreSQL dari Airflow
    postgres_conn_id = "postgres_conn_id"
    postgres_conn = BaseHook.get_connection(postgres_conn_id)
    postgres_conn_str = f"postgresql+psycopg2://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
    engine = create_engine(postgres_conn_str)

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
    # Menentukan dataset dan tabel BigQuery
    dataset_id = "ecommers_de4_team_2"
    table_id = "raw_products"
    table_ref = client.dataset(dataset_id).table(table_id)

    # Query untuk mengambil data
    query = "SELECT * FROM products"
    df = pd.read_sql(query, engine)

    df["harga"] = pd.to_numeric(df["harga"], errors="coerce")
    df["harga_dasar"] = pd.to_numeric(df["harga"], errors="coerce")
    df.rename(columns={"harga": "price"}, inplace=True)
    df.rename(columns={"harga_dasar": "base_price"}, inplace=True)

    # Load data ke BigQuery
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()
    print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")
