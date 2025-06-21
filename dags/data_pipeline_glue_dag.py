## This code is part of a data pipeline that uses AWS Glue to process data. This dag will
## be trigered by an SQS message indicating that new data is availible in s3 bucket.

from airflow import DAG
from airflow.operators.python import PythonOperator
import logging  


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
    start_date=None,  # Set to None to avoid scheduling issues
    tags=['data_pipeline', 'glue']
) as dag:


    
    start = PythonOperator(
        task_id='start',
        python_callable=lambda: logging.info("Starting data pipeline with Glue"),
        dag=dag
    )

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: logging.info("Data pipeline with Glue completed"),
        dag=dag
    )
    
    start >> end
