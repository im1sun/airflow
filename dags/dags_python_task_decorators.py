from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task


with DAG(
    dag_id="dags_python_task_decorators",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context.override(task_id="python_task_1")("task_decorator 실행")