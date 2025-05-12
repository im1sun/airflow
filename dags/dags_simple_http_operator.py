from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_simple_http_operator",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    '''서울시 송파구 CCTV 장소 정보'''
    start_task = BashOperator(
        task_id="start_task", 
        bash_command="echo start!"    
    )

    tb_cctv_info = HttpOperator(
        task_id="tb_cctv_info",
        http_conn_id="openapi.seoul.go.kr",
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/TbOpendataFixedcctvSP/1/15/',
        method='GET',
        headers={'Content-Type': 'application/json',
                    'charset': 'utf-8',
                    'Accept': '*/*'
                }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cctv_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_cctv_info >> python_2()
