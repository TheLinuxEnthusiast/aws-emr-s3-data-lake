import os
import configparser
from datetime import datetime
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql import SparkSession, SQLContext
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

#Global variables are set, so data is visible to log processing function
artist_data=None
song_data=None

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
    if AWS_SECRET_KEY != "" and AWS_ACCESS_KEY != "":
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    #creating the context
    sqlContext = SQLContext(sc)
    
    return sqlContext



def process_song_data(spark, input_data, output_data):
    """
    Description: Processes song and artist data that writes a parquet file to S3
    
    Arguments:
        spark - spark SQL context obect
        input_data - path to S3 input bucket
        output_data - path to S3 output bucket
    
    Returns:
        None
    """
    
    global artist_data
    global song_data
    
    song_schema = StructType([StructField("artist_id",StringType(),True), \
                         StructField("artist_latitude",DoubleType(),True), \
                         StructField("artist_location",StringType(),True), \
                         StructField("artist_longitude",DoubleType(),True), \
                         StructField("artist_name",StringType(),True), \
                         StructField("duration",DoubleType(),True), \
                         StructField("num_songs",LongType(),True), \
                         StructField("song_id",StringType(),True), \
                         StructField("title",StringType(),True), \
                         StructField("year",LongType(),True)])
    
    song_path = input_data + "song_data/A/A/[A-Z]/*.json"
    
    songs = spark.read.json(song_path, schema = song_schema)

    song_data = songs.select(["song_id","title","artist_id","year","duration"]).distinct()

    song_data = song_data.repartition(6)
    song_data.write.format("parquet").partitionBy("year", "artist_id").mode("overwrite").save(output_data + "dim_song/")

    artist_data= songs.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]).distinct()

    artist_data = artist_data.repartition(6)
    artist_data.write.format("parquet").partitionBy("artist_name").mode("overwrite").save(output_data + "dim_artist/")


    
def process_log_data(spark, input_data, output_data):
    """
    Description: Processes log files and writes them out as parquet files in S3.
    
    Arguments:
        spark - Spark SQL context Object
        input_data - Input log data path to S3
        output_data - Output log data path to S3
    
    Returns:
        None
    """
    
    global artist_data
    global song_data
    
    log_path = input_data + "log_data/2018/11/2018-11-13-events.json"
    
    log_data = spark.read.json(log_path)
  
    users_table = log_data \
                  .select(["userId", "firstName", "lastName", "gender", "level"]) \
                  .withColumnRenamed("userId", "user_id") \
                  .withColumnRenamed("firstName", "first_name") \
                  .withColumnRenamed("lastName", "last_name") \
                  .distinct() 
    
    users_table = users_table.repartition(6)
    users_table.write.format("parquet").partitionBy("user_id").mode("overwrite").save(output_data + "dim_user/")

    get_hour = udf(lambda x: x.hour)
    get_day = udf(lambda x: x.day)
    get_week = udf(lambda x: x.isocalendar()[1])
    get_month = udf(lambda x: x.month)
    get_year = udf(lambda x: x.year)
    get_weekday = udf(lambda x: x.isoweekday())
    to_timestamp = udf(lambda x: int(x.timestamp()*1000))
    to_datetime = udf(lambda x : datetime.utcfromtimestamp(x/1000.0))
    
    time_df = log_data.withColumn("ts_m", to_datetime("ts"))
    
    time_table = time_df.select(["ts_m"]).withColumn("ts", to_timestamp(time_df.ts_m)) \
                        .withColumn("hour", get_hour(time_df.ts_m)) \
                        .withColumn("day", get_day(time_df.ts_m)) \
                        .withColumn("week", get_week(time_df.ts_m)) \
                        .withColumn("month", get_month(time_df.ts_m)) \
                        .withColumn("year", get_year(time_df.ts_m)) \
                        .withColumn("weekday", get_weekday(time_df.ts_m)) \
                        .select(["ts","hour","day","week","month","year","weekday"]) \
                        .distinct()
    
    time_table = time_table.repartition(6)
    time_table.write.format("parquet").partitionBy("year", "month").mode("overwrite").save(output_data + "dim_time/")

    songplay = log_data.select(["ts", "userId", "level", "song", "artist", "sessionId", "location", "userAgent"]).distinct()

    # Filter global datasets for artist and songs
    artist_only = artist_data.select(["artist_id","artist_name"]).distinct()
    songs_only = song_data.select(["song_id","title"]).distinct()
    
    songplays_table = songplay \
                 .join(artist_only, artist_only.artist_name == songplay.artist, 'inner') \
                 .join(songs_only, songs_only.title == songplay.song, 'inner') \
                 .join(time_table, time_table.ts == songplay.ts, 'inner') \
                 .select(songplay.ts, time_table.year, time_table.month, songplay.userId, songplay.level, songs_only.song_id, artist_only.artist_id, songplay.sessionId, songplay.location, songplay.userAgent) \
                 .withColumnRenamed("userId", "user_id") \
                 .withColumnRenamed("sessionId", "session_id")
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.repartition(6)
    songplays_table.write.format("parquet").partitionBy("year", "month").mode("overwrite").save(output_data + "fact_songPlay/")


def main():
    """
    Description: Main execution function for ETL.
    
    Arguments:
        None
    
    Returns:
        None
    """
    
    spark = create_spark_session()
    
    process_song_data(spark, S3_INPUT, S3_DESTINATION)    
    process_log_data(spark, S3_INPUT, S3_DESTINATION)
    
    spark.stop()


if __name__ == "__main__":
    main()
