from airflow.operators.sensors import S3PrefixSensor
from airflow.operators.bash_operator import BashOperator

from airflow import DAG

with DAG('s3_event') as dag:

    t1 = S3PrefixSensor(
        task_id='s3_sensor',
        bucket_name='airflow-input-coke',
        prefix='',
        start_date='@today',
        dag=dag)
    
    t2 = BashOperator(
        task_id='print_key',
        bash_command='echo "I Win"',
        start_date='@today',
        dag=dag
    )

    t1 >> t2
