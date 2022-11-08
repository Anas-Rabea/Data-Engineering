import configparser
from datetime import datetime
import os, time
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, unix_timestamp,lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from io import StringIO
from zipfile import ZipFile
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= ''
os.environ['AWS_SECRET_ACCESS_KEY']= ''

def create_spark_session():
    """
    create spark session with apache hadoop framworks
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    process song data from s3 bucket and redesign with adding some dimension tables.
    spark: spark session.
    input_data: input path of data to read from
    output_data: output path to save 
    """
    # get filepath to song data file
#     input_data = "s3a://udacity-dend/"
    song_data = input_data + 'song_data/A/A/A/*.json'
    #create new schema to add during reading our data
    Schema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",Str()),
    Fld("duration",Dbl()),
    Fld("num_songs",Dbl()),
    Fld("song_id",Str()),
    Fld("title",Str()),
    Fld("year",Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema = Schema)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','year','artist_id','duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data +'songs/')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name','artist_latitude','artist_location','artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')

def process_log_data(spark, input_data, output_data):
    
    """
    process logs data from s3 bucket and redesign with adding some dimension tables.
    spark: spark session.
    input_data: input path of data to read log data from
    output_data: output path to save 
    """
    


    # get filepath to log data file
    log_data = 's3a://udacity-dend/log_data/*/*/*.json'


    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.filter("page == 'NextSong'").dropDuplicates()

    # extract columns for users table    
    user_table = df.select(['firstName','gender','userId','level','lastName']).dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    new_df_logs = df.withColumn("timestamp",get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    pattern = r'(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+).+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+'
    new_df_logs = new_df_logs.withColumn('timest', regexp_replace('timestamp', pattern, '$1-$2-$3 $4:$5:$6').cast('timestamp'))
    new_df_logs = new_df_logs.withColumn('month',month(new_df_logs.timest))\
        .withColumn("hour",hour(new_df_logs.timest))\
        .withColumn("year",year(new_df_logs.timest))\
        .withColumn("day",dayofmonth(new_df_logs.timest))\
        .withColumn("week",weekofyear(new_df_logs.timest))\
        .withColumn('weekday', dayofweek('timest'))
    
    # extract columns to create time table
    time_table = new_df_logs.select(["timest"])\
        .withColumn("hour",hour(new_df_logs.timest))\
        .withColumn("month",month(new_df_logs.timest))\
        .withColumn("year",year(new_df_logs.timest))\
        .withColumn("day",dayofmonth(new_df_logs.timest))\
        .withColumn("week",weekofyear(new_df_logs.timest))\
        .withColumn('weekday', dayofweek('timest')).dropDuplicates()

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')


    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/')
    df10 = song_df.join(artists_table,(song_df.artist_id == artists_table.artist_id))\
        .drop(song_df.artist_id)\
        .drop(song_df.year)
    df20 = df10.join(new_df_logs,(df10.artist_name == new_df_logs.artist))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df20.select(\
    col('timest').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'))\
        .repartition("year", "month").dropDuplicates()


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')

def main():
    """
    create spark session and run song and log processing functions
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-result/"

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

main()

#users_df = spark.read.parquet(output_data + 'users/')

#users_df.show(3)

#songplays_df = spark.read.parquet(output_data + 'songplays/')

#songplays_df.show(3)



