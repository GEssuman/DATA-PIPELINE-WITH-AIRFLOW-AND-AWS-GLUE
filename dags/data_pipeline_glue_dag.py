## This code is part of a data pipeline that uses AWS Glue to process data. This dag will
## be trigered by an SQS message indicating that new data is availible in s3 bucket.

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging  
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException


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
    # schedule="@daily",  # Adjust as needed
    schedule="*/10 * * * *",  # Set to None to avoid scheduling issues
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
            WaitTimeSeconds=20
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
        import pandas as pd
        bucket = s3_obj['bucket']
        key = s3_obj['key']
        s3_object_path = f"s3://{bucket}/{key}"
        logging.info(f"Validating file: s3://{s3_object_path}")
        try:
            reader = pd.read_csv(s3_object_path, chunksize=500)
            first_chunk = next(reader)

            required_columns = {"user_id", "track_id", "listen_time"}
            if not required_columns.issubset(set(first_chunk.columns)):
                raise ValueError(f"Missing required columns in {s3_object_path}")

            s3_obj['validated'] = True  # Mark as validated
            return s3_obj
        except Exception as e:
            logging.error(f"Validation failed for {key} in bucket {bucket}: {e}")
            s3_obj['validated'] = False

        return s3_obj 
    
    @task
    def process_data(extracted_data: dict):
        import pandas as pd
        bucket = extracted_data['bucket']
        key = extracted_data['key']
        is_valid = extracted_data['validated']
        s3_object_path = f"s3://{bucket}/{key}"
        file_name = key.split('/')[-1]
        s3_processed_path = f"s3://{bucket}/processed/{file_name}"  # Save processed data in a new location
        if not is_valid:
            logging.error(f"Data validation failed for {key} in bucket {bucket}. Skipping processing.")
            raise AirflowSkipException(f"Validation failed for {key}")
            
        logging.info(f"Processing data from bucket: {bucket}, key: {key}")
        try:
            df = pd.read_csv(s3_object_path, nrows=20)
            df.to_csv(s3_processed_path, index=False)
            return {
                'bucket': bucket,
                'key': "processed/" + file_name,
                'processed': True  # Placeholder for processed data
            }
        except Exception as e:
            logging.error(f"Processing failed for {key} in bucket {bucket}: {e}")
            raise


    @task
    def load_to_dynamo(processed_data: dict):
        bucket = processed_data['bucket']
        key = processed_data['key']
        is_processed = processed_data.get('processed', False)
        table_name = "music_streaming_records"
        try:

            logging.info(f"Loading file from s3://{bucket}/{key} into DynamoDB {table_name}")
            if not is_processed:
                raise AirflowSkipException(f"Processing not completed for {key}")
        except Exception as e:
            logging.error(f"Failed to load data into DynamoDB: {e}")
            raise
            

    @task
    def archive_data(source_data: dict):
        import boto3
        source_bucket = source_data['bucket']
        source_key = source_data['key']
        
        archive_bucket = "music-streaming-archive.amalitech-gke"
        archive_key = f"archived/{source_key}"

        logging.info(f"📦 Archiving s3://{source_bucket}/{source_key} to s3://{archive_bucket}/{archive_key}")
        s3 = boto3.client('s3')
        try:
            # Step 1: Copy
            s3.copy_object(
                Bucket=archive_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=archive_key
            )

            # Step 2: Delete original
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            logging.info(f"Successfully archived {source_key} to {archive_bucket}/{archive_key}")
        
        except Exception as e:
            logging.error(f"Move failed: {e}")
            raise

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: logging.info("Data pipeline with Glue completed"),
        dag=dag
    )



    extract_data = fetch_sqs_messages()
    validated_data = validate_data.expand(s3_obj=extract_data)
    processed_data = process_data.expand(extracted_data=validated_data)
    load_to_dynamo = load_to_dynamo.expand(processed_data=processed_data)
    archive_data = archive_data.expand(source_data=validated_data)  

    
    extract_data >> validated_data >> processed_data
    processed_data >> [load_to_dynamo, archive_data] >> end
    
   