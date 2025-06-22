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
    transformed_df = glueContext.create_dynamic_frame.from_options(
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [f"{input_path}"], "recurse": True}
        ).toDF()

    transformed_df.createOrReplaceTempView("streamed_music_tb")

    stats_df = spark.sql("""
    WITH genre_stats AS (
    SELECT
        DATE_TRUNC('day', listen_time) AS day,
        track_genre,
        COUNT(*) AS listen_count,
        COUNT(DISTINCT user_id) AS unique_listeners,
        SUM(duration_ms) AS total_listen_time
    FROM streamed_music_tb
    GROUP BY day, track_genre
    )
    SELECT
        *,
        ROUND(
    CASE 
        WHEN unique_listeners > 0 
        THEN total_listen_time * 1.0 / unique_listeners
        ELSE 0
    END, 5
    ) AS avg_listen_time_per_user
    FROM genre_stats
    ORDER BY day, track_genre;
    """)

    top_song_df = spark.sql("""
    SELECT *
    FROM (
        SELECT
            DATE_TRUNC('day', listen_time) AS day,
            track_genre,
            track_name,
            COUNT(*) AS play_count,
            ROW_NUMBER() OVER (
                PARTITION BY DATE_TRUNC('day', listen_time), track_genre
                ORDER BY COUNT(*) DESC
            ) AS rank
        FROM streamed_music_tb
        GROUP BY DATE_TRUNC('day', listen_time), track_genre, track_name
        ) ranked
        WHERE rank <= 3
        ORDER BY day, track_genre, rank;"""
    )
        
    stats_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/genre_stats")
    top_song_df.write.mode("overwrite").option("header", "true").csv(f"{output_path}/top_songs_per_genre")
   
    logging.info("Transformation completed successfully and data written to S3.")

except Exception as e:
    print(f"Error during transformation: {e}")
    
