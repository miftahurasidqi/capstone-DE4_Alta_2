from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from airflow.models import Variable
from docker.types import Mount
from airflow import DAG

from extract.sales_transactions import extract_sales_transactions
from extract.channel_performance import extract_channel_performance
from extract.customer_acquisition_costs import extract_customer_acquisition_costs



dag = DAG(
    dag_id="extract_e_commerce_data",
    description="extract data e-comerce from csv, postgres and load to bigquery",
    schedule_interval="@daily",
    start_date=datetime(2024, 8, 21),
    catchup=False,
)

start = DummyOperator(task_id="start")

load_customer_acquisition_costs_task = PythonOperator(
    task_id="load_customer_acquisition_costs_task",
    python_callable=extract_customer_acquisition_costs,
    op_kwargs=None,
    dag=dag
)

load_channel_performance_task = PythonOperator(
    task_id="load_channel_performance_task",
    python_callable=extract_channel_performance,
    op_kwargs=None,
    dag=dag
)

load_sales_transactions_task = PythonOperator(
    task_id="load_sales_transactions_task",
    python_callable=extract_sales_transactions,
    op_kwargs=None,
    dag=dag
)

finish = DummyOperator(task_id="finish")

start >> load_customer_acquisition_costs_task >> load_channel_performance_task >> load_sales_transactions_task >> finish