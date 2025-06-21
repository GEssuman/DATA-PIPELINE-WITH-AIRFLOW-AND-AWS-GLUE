import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'key'])
print("Arguments received:", args)
# print("Glue job started successfully")

job_name = args['JOB_NAME']
bucker = args['bucket']
key = args['key']
print(f"Job Name: {job_name}, Bucket: {bucker}, Key: {key}")
try:
    stream_df = spark.read.csv(f"s3://{bucker}/{key}", header=True)

    user_df = spark.read.csv("s3://music-streaming.amalitech-gke/users/users.csv", header=True)
    song_df = spark.read.csv("s3://music-streaming.amalitech-gke/songs/songs.csv", header=True)


    cleaned_song_df = song_df.drop(*["explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "intrumentalness", "liveness", "valence", "tempo", "time_signature"])
    cleaned_user_df = user_df.drop(*["user_age", "user_country", "created_at"])



    transformed_df = stream_df.join(cleaned_song_df, on="track_id", how="left").join(cleaned_user_df, on="user_id", how="left")
    transformed_df = transformed_df.fillna(value="Unknown", subset =["artists", "album_name"])


    transformed_df.drop_duplicates()


    transformed_df.createOrReplaceTempView("streamed_music_tb")


    spark.sql("""
    SELECT
        DATE_TRUNC('day', listen_time) AS day,
        track_genre,
        COUNT(*) AS listen_count,
        COUNT(DISTINCT user_id) AS unique_listeners,
        SUM(duration_ms) AS total_listen_time,
        ROUND(SUM(duration_ms) * 1.0 / COUNT(DISTINCT user_id), 2) AS avg_listen_time_per_user
    FROM streamed_music_tb
    GROUP BY DATE_TRUNC('day', listen_time), track_genre
    ORDER BY day, track_genre;""").show()
except Exception as e:
    print(f"Error during transformation: {e}")
    
