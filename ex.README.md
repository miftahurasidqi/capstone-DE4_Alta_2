# Capstone-project-ELT: ELT pipeline data model for Green Motion

## Background Scenario

lorem ipsum dolor

## Tools

- Postges (Data Source 1)
- CSV (Data Source 2)
- BigQuery (Data Warehouse)
- Python (Ingestion)
- Airflow (Orchestration)
- DBT (Transformation)

## Data Pipeline Design

This data pipeline uses the ELT concept, because it uses Python as an ingestion tool
![data_pipeline](assets/pipeline_data.png)

## ERD

![erd](assets/ERD_logical.png)

## Clone This Repository

```
git clone https://github.com/miftahurasidqi/capstone-DE4_Alta_2.git
```

## Ingesting data on Airflow width PythonOperator

Run docker [compose](airbyte/docker-compose.yml) to use Airflow

```
docker compose up -d
```

Then open `localhost:8000` to access Airbyte.

```
Username: airbyte
Password: password
```

Create your source.

![source](assets/create_source.PNG)

Create your destination.

![destination](assets/create_destination.PNG)

Connect your data source with your data destination on Airbyte. At this stage you can schedule the data load.

![conection_airbyte](assets/create_conection_on_airbyte.PNG)

## Data Modeling on DBT

### Install using pip and virtual environments

Create new venv

```
python -m venv dbt_venv              # create the environment
```

Activate virtual environment

```
dbt_venv\Scripts\activate            # activate the environment for Windows
```

### Install and Setup dbt

Install dbt-bigquery

```
python -m pip install dbt-bigquery
```

Run dbt cli to init dbt with BigQuery as data platform

```
dbt init my_dbt_project
```

Testing dbt connection. Make sure you are in your DBT project directory when testing the connection

```
dbt debug
```

Setup DBT Project configuration

```
models:
  my_dbt_project:
    # Config indicated by + and applies to all files under models/example/
    staging:
      +materialized: table
      +schema: staging
    intermediate:
      +materialized: table
      +schema: intermediate
    dimensional:
      +materialized: table
      +schema: dimensional
    fact:
      +materialized: table
      +schema: fact
    data_mart:
      +materialized: table
      +schema: data_mart
```

Defining Source and creating your a Model

```
version: 2

sources:
  - name: source
    tables:
      - name: DISCOUNT_KUPON
      - name: customers
      - name: marketing_spend
      - name: online_sales

models:
  - name: stg_disc_kupon
    description: "A starter dbt model"
    columns:
      - name: COUPON_CODE
        description: "The primary key for this table"
        data_tests:
          - not_null

  - name: stg_customers
    description: "A customers staging model"
    columns:
      - name: CustomerID
        description: "The primary key for this table"
        data_tests:
          - not_null
          - unique

  - name: stg_marketing_spend
    description: "A marketing_spend staging model"
    columns:
      - name: Date
        description: "The primary key for this table"
        data_tests:
          - not_null

  - name: stg_online_sales
    description: "A online sales staging model"
    columns:
      - name: Transaction_ID
        description: "The primary key for this table"
        data_tests:
          - not_null
```

### Run and test your model

Once you create a model, you can then run your model

```
dbt run
dbt test
```

### Results after creating the model

This is the result on your bigquery after running dbt successfully

![reult_dbt](assets/data%20warehouse.PNG)

## dbt automation with airflow

Before you run the Astro CLI you need to [download](https://github.com/astronomer/astro-cli/releases) the installer and add the installer path to your local environment variables.

<b>Create an Astro project</b>
</br>

```
astro dev init
```

This command generates all of the project files you need to run Airflow locally, including example DAGs that you can run out of the box.

<b>Run Airflow locally</b>
</br>
Before you run Airflow locally you need to add the following command to the [dockerfile](dbt-project/Dockerfile).

```
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery==1.8.1 && deactivate
```

To start running your project in a local Airflow environment, run the following command from your project directory:

```
astro dev start
```

After your project builds successfully, open the Airflow UI in your web browser at `https://localhost:8080/` with credetial.

```
username : admin
password : admin
```

<b>Create Dags for data modeling</b>
</br>
Make sure you are in the dags directory and create the [bg_dbt.py](dbt-project/dags/bg_dbt.py) file to run your dbt automation and scheduling.

```
from datetime import timedelta
from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, Dataset

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


dbt_dataset = Dataset("dbt_load")

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1, 0, 0, 0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "scheduled_dbt_bg_daily",
    default_args=default_args,
    catchup=False,
    schedule=[dbt_dataset],
)

# dbt_check = BashOperator(
#     task_id="dbt_check",
#     bash_command="cd /usr/local/airflow/include/my_dbt_project; source /usr/local/airflow/dbt_venv/bin/activate; dbt run --profiles-dir /usr/local/airflow/include/my_dbt_project/dbt-profiles/",
#     dag=dag,
# )

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /usr/local/airflow/include/my_dbt_project; source /usr/local/airflow/dbt_venv/bin/activate; dbt run --profiles-dir /usr/local/airflow/include/my_dbt_project/dbt-profiles/",
    dag=dag,
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /usr/local/airflow/include/my_dbt_project; source /usr/local/airflow/dbt_venv/bin/activate; dbt test --profiles-dir /usr/local/airflow/include/my_dbt_project/dbt-profiles/",
    dag=dag,
)

with dag:
    dbt_run >> dbt_test
```

<b>Triger DAG</b>
</br>
![dags](assets/DBT_Dags.PNG)

## Marketing campaign analysis visualization

visualization created using looker studio. you can see it [here](https://lookerstudio.google.com/reporting/8e84651e-7b06-4c6d-99dd-e5e490a5347c)
