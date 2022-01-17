import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView("song_data_table")
    
    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT DISTINCT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM song_data_table
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').mode('overwrite').parquet(output_data+'/songs')

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT DISTINCT 
            artist_id, 
            artist_name AS name, 
            artist_location AS location, 
            artist_latitude AS latitude, 
            artist_longitude AS longitude
        FROM song_data_table
    ''')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'/artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page=='NextSong'")
    
    # extract columns for users table
    df.createOrReplaceTempView("log_data_table")
    user_table = spark.sql('''
        SELECT DISTINCT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
        FROM log_data_table
    ''')
    
    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(output_data+'/users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000.0))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn('date', get_datetime(df.timestamp))
    
    df.createOrReplaceTempView('log_data_table')
    
    spark.udf.register("get_hour", lambda x: x.hour)
    spark.udf.register("get_day", lambda x: x.day)
    spark.udf.register("get_week", lambda x: x.strftime('%W'))
    spark.udf.register("get_year", lambda x: x.year)
    spark.udf.register("get_month", lambda x: x.month)
    spark.udf.register("get_weekday", lambda x: x.weekday())
                              
    # extract columns to create time table
    time_table = spark.sql('''
        SELECT DISTINCT
            CAST (date AS timestamp)  AS start_time,
            CAST (get_hour(date) AS int) AS hour,
            CAST (get_day(date) AS int) AS day,
            CAST (get_week(date) AS int) AS week,
            CAST (get_month(date) AS int) AS month,
            CAST (get_year(date) AS int) AS year,
            CAST (get_weekday(date) AS int) AS weekday
        FROM log_data_table
    ''')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode('overwrite').parquet(output_data+'/time')

    # read in song data to use for songplays table
    song_df = spark.sql('''
        SELECT 
            *, 
            CAST (get_month(date) AS int) AS month,
            CAST (get_year(date) AS int) AS year
        FROM log_data_table
    ''')
                              
    song_df.createOrReplaceTempView("log_data_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        SELECT DISTINCT
            l.date AS start_time,
            l.userId AS user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId AS session_id,
            l.location,
            l.userAgent,
            l.month,
            l.year
        FROM log_data_table l
        JOIN song_data_table s
        ON l.song = s.title
    ''')
    
    # add songplay_id
    df_tmp = songplays_table.rdd.zipWithIndex().toDF()
    songplays_table = df_tmp.select(col("_1.*"),col("_2").alias('songplay_id'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode('overwrite').parquet(output_data+'/songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://publicbucketphi"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
