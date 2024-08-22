from google.oauth2 import service_account
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from google.cloud import bigquery
import pandas as pd
import json


def extract_customers():
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
    table_id = "raw_customers"
    table_ref = client.dataset(dataset_id).table(table_id)

    # Query untuk mengambil data
    query = "SELECT * FROM customers_august"
    df = pd.read_sql(query, engine)

    # Load data ke BigQuery
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()
    print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")
