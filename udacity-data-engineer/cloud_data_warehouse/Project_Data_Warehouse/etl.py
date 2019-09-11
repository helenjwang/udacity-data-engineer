
import configparser
import datetime
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id

'''
Fetch AWS IAM Credentials from dl.cfg
'''
#ADD A HEADER TO THE .cfg config file i.e [AWS]
config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

'''Return spark session
Initiate a spark session on AWS hadoop
'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''Return none
Input: spark session, song_input_data json, and output_data S3 path
Purpose: Process song_input data using spark and output as parquet to S3
'''
def process_song_data(spark, song_input_data, output_data):
    # get filepath to song data file
    print('Read song data from json file')
    song_data = spark.read.json(song_input_data)
    
    # read song data file
    print('Print song data schema')
    song_df = song_data
    print(song_df.count())
    song_df.printSchema()

    # extract columns to create songs table
    print('Extract columns to create song table')
    artist_id = "artist_id"
    artist_latitude = "artist_latitude"
    artist_location = "artist_location"
    artist_longitude = "artist_longitude"
    artist_name = "artist_name"
    duration = "duration"
    num_songs = "num_songs"
    song_id = "song_id"
    title = "title"
    year = "year"