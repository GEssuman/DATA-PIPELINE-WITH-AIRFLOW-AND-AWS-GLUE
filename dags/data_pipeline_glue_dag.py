## This code is part of a data pipeline that uses AWS Glue to process data. This dag will
## be trigered by an SQS message indicating that new data is availible in s3 bucket.
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging  
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.docker.operators.docker import DockerOperator


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
    schedule="@daily",  # Set to None to avoid scheduling issues
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
    def prepare_glue_job_tranformation_args(validated_files: list[dict]):
        ts = datetime.now().strftime("%Y%m%d%H%M%S")

        glue_args = []

        for file in validated_files:
            if file.get('validated'):
                input_path = f"s3://{file['bucket']}/{file['key']}"
                output_key = f"processed/{file['key'].replace('/', '_')}{ts}"
                output_path = f"s3://{file['bucket']}/{output_key}"
                glue_args.append({
                    "script_args": {
                    "--input_path": input_path,
                    "--output_path": output_path,
                    "--bucket": file['bucket'],
                    "--key": file['key'],
                    "--output_key": output_key
                    }
                })
        if not glue_args:
            logging.warning("No validated files found for Glue job arguments.")
            return []
        return glue_args
    
    @task
    def prepare_glue_job_kpi_args(glue_job_tranformation_args: list[dict]):
        ts = datetime.now().strftime("%Y%m%d%H%M%S")

        glue_args = []

        for file in glue_job_tranformation_args:
            input_path = file['script_args']['--output_path']
            output_key = f"presentation/{file['script_args']['--output_key']}{ts}"
            output_path = f"s3://{file['script_args']['--bucket']}/{output_key}"
            glue_args.append({
                "script_args": {
                    "--input_path": input_path,
                    "--output_path": output_path,
                    "--bucket": file['script_args']['--bucket'],
                    "--key": file['script_args']['--key'],
                    "--output_key": output_key
                }
            })
        if not glue_args:
            logging.warning("No Glue job arguments found for KPI calculation.")
            return []
        return glue_args
    
    @task
    def prepare_load_to_dynamodb_args(glue_job_kpi_args: list[dict]):
        

        glue_args = []

        for file in glue_job_kpi_args:
            input_path = file['script_args']['--output_path']
            genre_stats_input_path = f"{input_path}/genre_stats"
            top_songs_per_genre_input_path = f"{input_path}/top_songs_per_genre"
            genre_stats_table_name = "genre_stats"
            top_songs_table_name = "top_songs_per_genre"
            glue_args.append({
                "genre_stats": {
                    "input_path": genre_stats_input_path,
                    "table_name": genre_stats_table_name
                },
                "top_songs_per_genre": {
                    "input_path": top_songs_per_genre_input_path,
                    "table_name": top_songs_table_name
                }
            })
        if not glue_args:
            logging.warning("No Glue job arguments found for KPI calculation.")
            return []
        return glue_args




  

    @task
    def load_to_dynamo(job_kpi_args: dict):
        import boto3
        import pandas as pd
        from io import StringIO
        from urllib.parse import urlparse


        genre_stat = job_kpi_args['genre_stats']
        top_songs_per_genre = job_kpi_args['top_songs_per_genre']

        dynamodb = boto3.resource('dynamodb')
        s3 = boto3.client('s3')

        def is_s3_path(path):
            return path.startswith("s3://")

        def parse_s3_path(s3_path):
            """Parse s3://bucket/key into (bucket, prefix)"""
            parsed = urlparse(s3_path)
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")
            return bucket, prefix

        def list_csv_files(input_path):
            if is_s3_path(input_path):
                bucket, prefix = parse_s3_path(input_path)
                logging.info(f"Listing CSV files in s3://{bucket}/{prefix}")
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                if "Contents" not in response:
                    raise FileNotFoundError(f"No files found at {input_path}")
                return [
                    f"s3://{bucket}/{obj['Key']}"
                    for obj in response["Contents"]
                    if obj["Key"].endswith(".csv")
                ]
            else:
                return [os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith(".csv")]

        def read_csv_files(input_path):
            if input_path.endswith(".csv"):
                return read_single_csv(input_path)
            else:
                files = list_csv_files(input_path)
                if not files:
                    raise FileNotFoundError(f"No CSV files found in {input_path}")
                logging.info(f"Reading {len(files)} CSV files from {input_path}")
                return pd.concat((read_single_csv(f) for f in files), ignore_index=True)

        def read_single_csv(s3_path):
            if is_s3_path(s3_path):
                bucket, key = parse_s3_path(s3_path)
                obj = s3.get_object(Bucket=bucket, Key=key)
                return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            else:
                with open(s3_path, "r") as f:
                    return pd.read_csv(f)

        def load_file_to_table(input_path, table_name):
            try:
                logging.info(f"📥 Loading data from {input_path} into DynamoDB table '{table_name}'")
                df = read_csv_files(input_path)
                table = dynamodb.Table(table_name)

                count = 0
                for _, row in df.iterrows():
                    item = {k: str(v) if pd.notna(v) else None for k, v in row.to_dict().items()}
                    table.put_item(Item=item)
                    count += 1

                logging.info(f"✅ Successfully loaded {count} items into '{table_name}'")
            except Exception as e:
                logging.error(f"❌ Failed to load data into DynamoDB table '{table_name}': {e}")
                raise

        # Load genre stats and top songs
        load_file_to_table(genre_stat["input_path"], genre_stat["table_name"])
        load_file_to_table(top_songs_per_genre["input_path"], top_songs_per_genre["table_name"])



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
            raise AirflowSkipException(f"Archive failed: {e}")

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: logging.info("Data pipeline with Glue completed"),
        dag=dag
    )



    extract_data = fetch_sqs_messages()
    validated_data = validate_data.expand(s3_obj=extract_data)
    glue_job_args = prepare_glue_job_tranformation_args(validated_data)


    glue_jobs = GlueJobOperator.partial(
    task_id="run_glue_transformation_job",
    job_name="transformation_job",
    region_name="eu-north-1"
    ).expand_kwargs(glue_job_args)


    glue_job_kpi_args = prepare_glue_job_kpi_args(glue_job_args)
    glue_jobs_kpi = GlueJobOperator.partial(
        task_id="run_glue_kpi_job",
        job_name="kpi_implementation_job",
        region_name="eu-north-1"
    ).expand_kwargs(glue_job_kpi_args)

    load_to_dynamo_kpi_args = prepare_load_to_dynamodb_args(glue_job_kpi_args)
    load_to_dynamo = load_to_dynamo.expand(job_kpi_args=load_to_dynamo_kpi_args)
    archive_data = archive_data.expand(source_data=validated_data)  
    
    extract_data >> validated_data >> glue_job_args >> glue_jobs >> archive_data
    [glue_job_args, glue_jobs]>> glue_job_kpi_args
    glue_jobs_kpi >> load_to_dynamo_kpi_args
    [glue_jobs_kpi, load_to_dynamo_kpi_args] >> load_to_dynamo
    [ archive_data, load_to_dynamo] >> end
    
   