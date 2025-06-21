## This code is part of a data pipeline that uses AWS Glue to process data. This dag will
## be trigered by an SQS message indicating that new data is availible in s3 bucket.

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging  
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.decorators import task
import json
import boto3
import pandas as pd

default_args = {
    'owner': 'Essuman',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,  # Retry delay in seconds
}


with DAG(
    dag_id='data_pipeline_glue',
    default_args=default_args,
    description='A simple data pipeline using Glue',
    start_date=datetime(2024, 6, 21), # Set to None to avoid scheduling issues
    schedule="@daily",  # Adjust as needed
    max_active_runs=1,
    catchup=False,
    tags=['data_pipeline', 'glue']
) as dag:

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



    @task
    def fetch_sqs_messages():
        import boto3
        import json
        import logging

        sqs_url = "https://sqs.eu-north-1.amazonaws.com/309797288544/s3-music_stream_queue"
        sqs = boto3.client("sqs")

        response = sqs.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5
        )

        messages = response.get('Messages', [])
        if not messages:
            logging.info("No messages found in SQS")
            return []

        result = []
        for msg in messages:
            body = json.loads(msg['Body'])
            if "Records" in body:
                for record in body["Records"]:
                    result.append({
                        "bucket": record["s3"]["bucket"]["name"],
                        "key": record["s3"]["object"]["key"]
                    })

            # Optionally delete message after reading
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=msg['ReceiptHandle']
            )

        return result

    
    @task
    def validate_data(s3_obj: dict):
        bucket = s3_obj['bucket']
        key = s3_obj['key']
        logging.info(f"🔍 Validating file: s3://{bucket}/{key}")

        return s3_obj 
    
    @task
    def process_data(extracted_data: dict):
        bucket = extracted_data['bucket']
        key = extracted_data['key']
        logging.info(f"Processing data from bucket: {bucket}, key: {key}")

        return {
            'bucket': bucket,
            'key': key,
            'processed': True  # Placeholder for processed data
        }

    


    end = PythonOperator(
        task_id='end',
        python_callable=lambda: logging.info("Data pipeline with Glue completed"),
        dag=dag
    )


    @task
    def load_to_dynamo(processed_data: dict):
        bucket = processed_data['bucket']
        key = processed_data['key']
        table_name = "music_streaming_records"
        logging.info(f"Loading file from s3://{bucket}/{key} into DynamoDB {table_name}")

        

    @task
    def archive_data(source_data: dict):
        source_bucket = source_data['bucket']
        source_key = source_data['key']
        
        archive_bucket = "music-streaming-archive"
        archive_key = f"archived/{source_key}"  # Preserve original path

        logging.info(f"📦 Archiving s3://{source_bucket}/{source_key} to s3://{archive_bucket}/{archive_key}")
        # s3 = boto3.client('s3')
        # try:
        # # Step 1: Copy
        #     s3.copy_object(
        #         Bucket=archive_bucket,
        #         CopySource={'Bucket': source_bucket, 'Key': source_key},
        #         Key=archive_key
        #     )

        #     # Step 2: Delete original
        #     s3.delete_object(Bucket=source_bucket, Key=source_key)
        #     logging.info(f"Successfully archived {source_key} to {archive_bucket}/{archive_key}")
        
        # except Exception as e:
        #     logging.error(f"Move failed: {e}")
        #     raise

    extract_data = fetch_sqs_messages()
    validated_data = validate_data.expand(s3_obj=extract_data)
    processed_data = process_data.expand(extracted_data=validated_data)
    load_to_dynamo = load_to_dynamo.expand(processed_data=processed_data)
    archive_data = archive_data.expand(source_data=validated_data)  

    
    extract_data >> validated_data >> processed_data
    processed_data >> [load_to_dynamo, archive_data] >> end
    
   