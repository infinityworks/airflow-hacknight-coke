from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from datetime import datetime as dt

from airflow import DAG

with DAG('s3_event') as dag:

    t1 = S3KeySensor(
        task_id='s3_sensor',
        bucket_name='airflow-input-coke',
        bucket_key='*',
        start_date=dt.now(),
        dag=dag)
    
    t2 = BashOperator(
        task_id='print_key',
        bash_command='echo "I Win"',
        start_date=dt.now(),
        dag=dag
    )

    t1 >> t2
