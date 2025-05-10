from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_python_with_branch_decorator",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random

        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b', 'task_c']

    def common_func(**kwargs):
        print(kwargs['selected'])
    
    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    branch = select_random()  # Task 객체를 명확히 할당
    branch >> [task_a, task_b, task_c]