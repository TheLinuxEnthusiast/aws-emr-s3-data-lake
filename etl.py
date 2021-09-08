import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY=config.get('AWS', 'AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
S3_DESTINATION=config.get('S3', 'S3_DESTINATION')
S3_INPUT=config.get('S3', 'S3_INPUT')



"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
"""

def create_spark_session():
    """
    Description: Function that manages the creation of a spark session to the local cluster.
    
    Arguments:
        None
        
    Returns: 
        sqlContext - Spark Sql Context Object
    """
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0')
    sc = SparkContext(conf=conf)

    # add aws credentials
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    #creating the context
    sqlContext = SQLContext(sc)
    
    return sqlContext



def process_song_data(spark, input_data, output_data):
    """
    Description: Processes song and artist data and writes a parquet file to S3
    
    Arguments:
        spark - spark SQL context obect
        input_data - path to S3 input bucket
        output_data - path to S3 output bucket
    
    Returns:
        None
    """
    
    song_data = input_data + "song_data/"
    
    df = spark.read.json(song_data)

    songs_table = df.select(["song_id","title","artist_id","year","duration"]).distinct()

    songs_table.write.format("parquet").partitionBy("year", "artist_id").mode("overwrite").save(output_data + "dim_song/")

    artists_table = song_data.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]).distinct()

    artist_table.write.format("parquet").partitionBy("artist_name").mode("overwrite").save(output_data + "dim_artist/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    users_table = 
    
    # write users table to parquet files
    users_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    
    process_song_data(spark, S3_INPUT, S3_DESTINATION)    
    process_log_data(spark, S3_INPUT, S3_DESTINATION)


if __name__ == "__main__":
    main()
