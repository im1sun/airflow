from airflow.models.dag import DAG
import datetime
import pendulum
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator


with DAG(
    dag_id="dags_seoul_api_corona",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    '''서울시 송파구 CCTV 장소 정보'''
    
    tb_cctv_info = SeoulApiToCsvOperator(
        task_id="tb_cctv_info",
        dataset_nm = 'TbOpendataFixedcctvSP',
        path='/opt/airflow/files/TbCctvCountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbOpendataFixedcctvSP.csv'
    )

    # '''서울시 송파구 CCTV 장소 정보'''
    
    # tb_cctv_info = SeoulApiToCsvOperator2(
    #     task_id="tb_cctv_info",
    #     dataset_nm = 'TbCctvCountStatus',
    #     path='/opt/airflow/files/TbCctvCountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
    #     file_name='TbCctvCountStatus.csv'
    # )

    SeoulApiToCsvOperator
