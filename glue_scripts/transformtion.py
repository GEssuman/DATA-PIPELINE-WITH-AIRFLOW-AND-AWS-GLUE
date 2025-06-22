import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

import logging

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path', 'bucket', 'key'])
print("Arguments received:", args)
# print("Glue job started successfully")

job_name = args['JOB_NAME']
input_path = args['input_path']
output_path = args['output_path']
print(f"Job Name: {job_name}, input_path: {input_path}, output_path: {output_path}")

try:
    stream_df = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [f"{input_path}"], "recurse": True}
    ).toDF()


    user_df = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://music-streaming.amalitech-gke/users/users.csv"], "recurse": True}
    ).toDF()

    song_df = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://music-streaming.amalitech-gke/songs/songs.csv"], "recurse": True}
    ).toDF()




    cleaned_user_df = user_df.select("user_id", "user_name").dropDuplicates()
    cleaned_song_df = song_df.select("track_id", "track_name", "track_genre", "artists", "album_name", "duration_ms").dropDuplicates()


    transformed_df = (
        stream_df.join(cleaned_song_df, on="track_id", how="left") \
        .join(cleaned_user_df, on="user_id", how="left")
        )
    transformed_df = transformed_df.fillna(value="Unknown", subset =["artists", "album_name"])

    transformed_df.write.mode("overwrite").option("header", "true").csv(output_path)
    logging.info("Transformation completed successfully and data written to S3.")

except Exception as e:
    print(f"Error during transformation: {e}")
    
