from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from docker.types import Mount
from datetime import datetime
from airflow import DAG

from extract.csv.acquisition_costs import extract_acqusition_costs
from extract.csv.channel_performance import extract_channel_performance
from extract.postgres.sales_transactions import extract_sales_transactions
from extract.postgres.customers import extract_customers
from extract.postgres.channels import extract_channels
from extract.postgres.products import extract_products

dataset_id = "ecommers_de4_team_2"
local_path = Variable.get("localpath")

dag = DAG(
    dag_id="extract_e_commerce_data",
    description="extract data e-comerce from csv, postgres and load to bigquery",
    schedule_interval="@daily",
    start_date=datetime(2024, 8, 21),
    catchup=False,
)

start = DummyOperator(task_id="start")

# create table
create_bq_table_acqusition_costs = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_acqusition_costs",
    dataset_id=dataset_id,
    table_id="raw_acqusition_costs",
    schema_fields=[
        {"name": "channel_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "total_cost", "type": "FLOAT", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

create_bq_table_channel_performances = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_channel_performances",
    dataset_id=dataset_id,
    table_id="raw_channel_performances",
    schema_fields=[
        {"name": "channel_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "total_clicks", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "total_impressions", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
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

create_bq_table_customers = BigQueryCreateEmptyTableOperator(
    task_id="create_bq_table_customers",
    dataset_id=dataset_id,
    table_id="raw_customers",
    schema_fields=[
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "customer_name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
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
        {"name": "base_price", "type": "FLOAT", "mode": "NULLABLE"},
    ],
    gcp_conn_id="bigquery_con",
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
    ],
    gcp_conn_id="bigquery_con",
    dag=dag,
)

# extraxt load data
extract_channel_performance_data = PythonOperator(
    task_id="extract_channel_performance",
    python_callable=extract_channel_performance,
    op_kwargs=None,
    dag=dag,
)

extract_acqusition_costs_data = PythonOperator(
    task_id="extract_acqusition_costs",
    python_callable=extract_acqusition_costs,
    op_kwargs=None,
    dag=dag,
)

extract_channels_data = PythonOperator(
    task_id="extract_channels",
    python_callable=extract_channels,
    op_kwargs=None,
    dag=dag,
)

extract_customers_data = PythonOperator(
    task_id="extract_customers",
    python_callable=extract_customers,
    op_kwargs=None,
    dag=dag,
)

extract_products_data = PythonOperator(
    task_id="extract_products",
    python_callable=extract_products,
    op_kwargs=None,
    dag=dag,
)

extract_sales_transactions_data = PythonOperator(
    task_id="extract_sales_transactions",
    python_callable=extract_sales_transactions,
    op_kwargs=None,
    dag=dag,
)

# run dbt
dbt_run_cmd = DockerOperator(
    task_id="dbt_run_cmd",
    image="dbt_transform_capstone",
    container_name="dbt_container",
    api_version="auto",
    auto_remove=True,
    command="bash -c 'dbt run'",
    docker_url="tcp://docker-proxy:2375",
    network_mode="bridge",
    mounts=[
        Mount(source=f"{local_path}/dbt_transform", target="/usr/app", type="bind"),
        Mount(
            source=f"{local_path}/dbt_transform/profiles",
            target="/root/.dbt",
            type="bind",
        ),
    ],
    mount_tmp_dir=False,
    dag=dag,
)

dbt_test_cmd = DockerOperator(
    task_id="dbt_test_cmd",
    image="dbt_transform_capstone",
    container_name="dbt_container",
    api_version="auto",
    auto_remove=True,
    command="bash -c 'dbt test'",
    docker_url="tcp://docker-proxy:2375",
    network_mode="bridge",
    mounts=[
        Mount(source=f"{local_path}/dbt_transform", target="/usr/app", type="bind"),
        Mount(
            source=f"{local_path}/dbt_transform/profiles",
            target="/root/.dbt",
            type="bind",
        ),
    ],
    mount_tmp_dir=False,
    dag=dag,
)
end = DummyOperator(task_id="end")


start >> [
    create_bq_table_acqusition_costs,
    create_bq_table_channel_performances,
    create_bq_table_channels,
    create_bq_table_customers,
    create_bq_table_products,
    create_bq_table_sales_transactions,
]


create_bq_table_acqusition_costs >> extract_acqusition_costs_data
create_bq_table_channel_performances >> extract_channel_performance_data
create_bq_table_channels >> extract_channels_data
create_bq_table_customers >> extract_customers_data
create_bq_table_products >> extract_products_data
create_bq_table_sales_transactions >> extract_sales_transactions_data


(
    [
        extract_acqusition_costs_data,
        extract_channel_performance_data,
        extract_channels_data,
        extract_customers_data,
        extract_products_data,
        extract_sales_transactions_data,
    ]
    >> dbt_run_cmd
    >> dbt_test_cmd
    >> end
)
