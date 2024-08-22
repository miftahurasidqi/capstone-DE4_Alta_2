from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow import DAG

from extract.customer_acquisition_costs import extract_costumers_acqusition_costs
from extract.channel_performance import extract_channel_performance
from extract.sales_transactions import extract_sales_transactions
from extract.customers import extract_customers
from extract.channels import extract_channels
from extract.products import extract_products

dataset_id = "ecommers_de4_team_2"

dag = DAG(
    dag_id="extract_e_commerce_data",
    description="extract data e-comerce from csv, postgres and load to bigquery",
    schedule_interval="@daily",
    start_date=datetime(2024, 8, 21),
    catchup=False,
)

start = DummyOperator(task_id="start")

create_bq_table_channel_performances = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_channel_performances",
    dataset_id=dataset_id,
    table_id="raw_channel_performances",
    schema_fields=[
        {"name": "channel_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "total_transactions", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "total_revenue", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "total_clicks", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "total_impressions", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

extract_channel_performance_data = PythonOperator(
    task_id="extract_channel_performance",
    python_callable=extract_channel_performance,
    op_kwargs=None,
    dag=dag,
)

create_bq_table_costumers_acqusition_costs = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_costumers_acqusition_costs",
    dataset_id=dataset_id,
    table_id="raw_costumers_acqusition_costs",
    schema_fields=[
        {"name": "channel_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "total_cost", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "total_customers", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "cost_per_customer", "type": "FLOAT", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

extract_costumers_acqusition_costs_data = PythonOperator(
    task_id="extract_costumers_acqusition_costs",
    python_callable=extract_costumers_acqusition_costs,
    op_kwargs=None,
    dag=dag,
)

create_bq_table_channels = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_channels",
    dataset_id=dataset_id,
    table_id="raw_channels",
    schema_fields=[
        {"name": "channel_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "channel_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "type_channel", "type": "STRING", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

extract_channels_data = PythonOperator(
    task_id="extract_channels",
    python_callable=extract_channels,
    op_kwargs=None,
    dag=dag,
)

create_bq_table_customers = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_customers",
    dataset_id=dataset_id,
    table_id="raw_customers",
    schema_fields=[
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "customer_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "customer_address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

extract_customers_data = PythonOperator(
    task_id="extract_customers",
    python_callable=extract_customers,
    op_kwargs=None,
    dag=dag,
)

create_bq_table_products = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_products",
    dataset_id=dataset_id,
    table_id="raw_products",
    schema_fields=[
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

extract_products_data = PythonOperator(
    task_id="extract_products",
    python_callable=extract_products,
    op_kwargs=None,
    dag=dag,
)

create_bq_table_sales_transactions = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_sales_transactions",
    dataset_id=dataset_id,
    table_id="raw_sales_transactions",
    schema_fields=[
        {"name": "transaction_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "channel_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "unit_price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

extract_sales_transactions_data = PythonOperator(
    task_id="extract_sales_transactions",
    python_callable=extract_sales_transactions,
    op_kwargs=None,
    dag=dag,
)

end = DummyOperator(task_id="end")


(
    start
    >> create_bq_table_channel_performances
    >> create_bq_table_costumers_acqusition_costs
    >> create_bq_table_channels
    >> create_bq_table_customers
    >> create_bq_table_products
    >> create_bq_table_sales_transactions
    >> extract_channel_performance_data
    >> extract_costumers_acqusition_costs_data
    >> extract_channels_data
    >> extract_customers_data
    >> extract_products_data
    >> extract_sales_transactions_data
    >> end
)
