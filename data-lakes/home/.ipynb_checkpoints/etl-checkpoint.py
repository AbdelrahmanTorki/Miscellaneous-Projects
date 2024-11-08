import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth,
    hour,
    weekofyear,
    date_format,
    monotonically_increasing_id,
)

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    - Creates spark session with pre-set hard-coded configuration
    - Returns the created session
    """

    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Takes spark session, an s3 bucket to load the json files from
    as input_data and a destination s3 bucket to store data to as
    output_data.
    - Creates songs and artists data frames from songs_data json files.
    - Saves the created songs and artists data to the s3 bucket in
    output_data as parquet files applying necessary partitioning
    when applicable.
    """

    # get filepath to song data file

    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file

    df = spark.read.json(song_data)

    # extract columns to create songs table

    songs_table = df.select(
        ["song_id", "title", "artist_id", "year", "duration"]
    ).drop_duplicates(subset=["song_id"])

    # write songs table to parquet files partitioned by year and artist

    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(
        os.path.join(output_data, "songs.parquet")
    )

    # extract columns to create artists table

    artists_table = df.select(
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ).drop_duplicates(subset=["artist_id"])

    # write artists table to parquet files

    artists_table.write.mode("overwrite").parquet(
        os.path.join(output_data, "artists.parquet")
    )


def process_log_data(spark, input_data, output_data):
    """
    - Takes spark session, an s3 bucket to load the json files from
    as input_data and a destination s3 bucket to store data to as
    output_data.
    - Creates users, time, songplays data frames from log_data json
    files by applying necessary transformations.
    - Saves the created data to the s3 bucket located at output_data
    as parquet files partitioned by necessary columns when applicable.
    """

    # get filepath to log data file

    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file

    df = spark.read.json(log_data)

    # filter by actions for song plays

    df = df.where(df.page == "NextSong")

    # extract columns for users table

    users_table = df.select(
        ["userId", "firstName", "lastName", "gender", "level"]
    ).drop_duplicates(subset=["userId"])

    # write users table to parquet files

    users_table.write.mode("overwrite").parquet(
        os.path.join(output_data, "users.parquet")
    )

    # create timestamp column from original timestamp column

    get_timestamp = udf(lambda x: int(int(x) / 1000))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column

    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
    )
    df = df.withColumn("datetime", get_datetime(df.timestamp))

    # extract columns to create time table

    time_table = df.select(
        [
            col("datetime").alias("start_time"),
            hour("datetime").alias("hour"),
            dayofmonth("datetime").alias("day"),
            weekofyear("datetime").alias("week"),
            month("datetime").alias("month"),
            year("datetime").alias("year"),
            date_format("datetime", "E").alias("weekday"),
        ]
    ).drop_duplicates(subset=["start_time"])

    # write timetable to parquet files partitioned by year and month

    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(
        os.path.join(output_data, "time.parquet")
    )

    # read in song data to use for songplays table

    song_df = spark.read.json(
        os.path.join(input_data, "song_data/*/*/*/*.json")
    ).drop_duplicates()

    # extract columns from joined song and log datasets to create songplays table

    cond = [
        song_df.artist_name == df.artist,
        song_df.title == df.song,
        song_df.duration == df.length,
    ]
    songplays_table = (
        song_df.join(df.drop_duplicates(), cond)
        .drop_duplicates()
        .select(
            [
                monotonically_increasing_id().alias("songplay_id"),
                df.datetime.alias("start_time"),
                df.userId.alias("user_id"),
                df.level.alias("level"),
                song_df.song_id.alias("song_id"),
                song_df.artist_id.alias("artist_id"),
                df.sessionId.alias("session_id"),
                df.location.alias("location"),
                df.userAgent.alias("user_agent"),
                year(df.datetime).alias("year"),
                month(df.datetime).alias("month"),
            ]
        )
    )

    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(
        os.path.join(output_data, "songplays.parquet")
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
