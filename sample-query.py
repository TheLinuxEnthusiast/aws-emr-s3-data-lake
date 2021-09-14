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


def main(output_data):
    """
    Description: Executes a simple query against data written to S3
    
    Arguments:
        None
    
    Returns:
        None
    """
    
    spark = create_spark_session()
    
    #Read Parquet Files into spark from S3
    user_df = spark.read.parquet(output_data + "dim_user/")
    songPlay_df = spark.read.parquet(output_data + "fact_songPlay/")
    
    # Create temporary views for the data
    songPlay_df.createOrReplaceTempView("songplay")
    user_df.createOrReplaceTempView("users")
    
    query_result = spark.sql("""
    SELECT
        u.first_name,
        u.last_name,
        sp.user_id,
        COUNT(DISTINCT sp.session_id) as session_count
    FROM songplay sp
    JOIN users u
    ON u.user_id = sp.user_id
    GROUP BY 
        u.first_name,
        u.last_name,
        sp.user_id
    ORDER BY session_count DESC
    LIMIT 10
    """)
    
    query_result.limit(10).write.format("csv").mode("overwrite").save(output_data + "query_output/")
    
    spark.stop()


if __name__ == "__main__":
    main(S3_DESTINATION)
