## This code is part of a data pipeline that uses AWS Glue to process data. This dag will
## be trigered by an SQS message indicating that new data is availible in s3 bucket.

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging  
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor


default_args = {
    'owner': 'Essuman',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,  # Retry delay in seconds
    'catchup': False,
    'max_active_runs': 1,
    'wait_for_downstream': False
}


with DAG(
    'data_pipeline_glue',
    default_args=default_args,
    description='A simple data pipeline using Glue',
    start_date=datetime(2024, 6, 21), # Set to None to avoid scheduling issues
    schedule="@daily",  # Adjust as needed
    max_active_runs=1,
    tags=['data_pipeline', 'glue']
) as dag:



    wait_for_sqs = SqsSensor(
        task_id="check_sqs",
        sqs_queue="hhttps://sqs.eu-north-1.amazonaws.com/309797288544/s3-music_stream_queue",
        aws_conn_id="aws_default",
        max_messages=1,
        wait_time_seconds=10,
    )

        

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: logging.info("Data pipeline with Glue completed"),
        dag=dag
    )
    
    wait_for_sqs >> end