from sensors.seoul_api_date_sensor import SeoulApiDateSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    tb_lib_arrive_count_status_sensor = SeoulApiDateSensor(
        task_id='tb_lib_arrive_count_status_sensor',
        dataset_nm='SeoulLibNewArrivalInfo',
        base_dt_col='INDT',
        day_off=-3,
        poke_interval=600,
        mode='reschedule'
    )
    
    tb_lib_lecture_count_status_sensor = SeoulApiDateSensor(
        task_id='tb_lib_lecture_count_status_sensor',
        dataset_nm='SeoulLibraryLectureInfo',
        base_dt_col='TERM_START_DATE',
        day_off=0,
        poke_interval=600,
        mode='reschedule'
    )