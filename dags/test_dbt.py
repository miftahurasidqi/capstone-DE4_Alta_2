from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount

local_path = (
    "/home/miftahurasidqi/my_workspace/Data_engineer/DE_Alta_4/Capstone"
)

dag = DAG(
    dag_id="test",
    description="test",
    schedule_interval="@daily",
    start_date=datetime(2024, 8, 10),
    catchup=False,
)

test = dummy_task = DummyOperator(
    task_id="dummy_task",
    dag=dag,
)

dbt_debug_cmd = DockerOperator(
    task_id="dbt_debug_cmd",
    image="dbt_transform_capstone",
    container_name="dbt_container",
    api_version="auto",
    auto_remove=True,
    command="bash -c 'dbt debug'",
    docker_url="tcp://docker-proxy:2375",
    network_mode="bridge",
    mounts=[
        Mount(
            source=f"{local_path}/dbt_transform", target="/usr/app", type="bind"
        ),
        Mount(
            source=f"{local_path}/dbt_transform/profiles",
            target="/root/.dbt",
            type="bind",
        ),
    ],
    mount_tmp_dir=False,
    dag=dag,
)

# dbt_run_cmd = DockerOperator(
#     task_id="dbt_run_cmd",
#     image="dbt_in_docker_compose",
#     container_name="dbt_container",
#     api_version="auto",
#     auto_remove=True,
#     command="bash -c 'dbt run'",
#     docker_url="tcp://docker-proxy:2375",
#     network_mode="bridge",
#     mounts=[
#         Mount(
#             source=f"{local_path}/transformation", target="/usr/app", type="bind"
#         ),
#         Mount(
#             source=f"{local_path}/transformation/profiles",
#             target="/root/.dbt",
#             type="bind",
#         ),
#     ],
#     mount_tmp_dir=False,
# )

test >> dbt_debug_cmd

