from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator


# def create_tb_channel_performance(**kwargs):
#     hook = BigQueryHook(bigquery_conn_id="bigquery_default")
#     client = hook.get_client()

#     dataset_id = "ecommers_de4_team_2"
#     table_id = "raw_channel_performances"

#     dataset_ref = client.dataset(dataset_id)
#     table_ref = dataset_ref.table(table_id)

#     try:
#         client.get_table(table_ref)
#         print(f"Table {dataset_id}.{table_id} already exists.")
#     except Exception as e:
#         schema = [
#             {"name": "channel_id", "type": "STRING", "mode": "REQUIRED"},
#             {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
#             {"name": "total_transactions", "type": "INTEGER", "mode": "NULLABLE"},
#             {"name": "total_revenue", "type": "FLOAT", "mode": "NULLABLE"},
#             {"name": "total_clicks", "type": "INTEGER", "mode": "NULLABLE"},
#             {"name": "total_impressions", "type": "INTEGER", "mode": "NULLABLE"},
#         ]
#         table = bigquery.Table(table_ref, schema=schema)
#         client.create_table(table)
#         print(f"Table {dataset_id}.{table_id} created.")


# create_table_task = PythonOperator(
#     task_id="create_bq_table_if_not_exists",
#     python_callable=create_bq_table_if_not_exists,
#     dag=dag,
# )
