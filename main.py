from pyspark import SparkContext

#import findspark

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# import configparser

import argparse

def use_pip_modules(spark_context):

    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", "your_aws_access_id")
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", "your_aws_access_key")

    sqlContext = SQLContext(spark_context)

    df = sqlContext.read.json("s3n://bigdata-testing123/ethPrice_3.json")
    df.show()



if __name__ == '__main__': 



    sc = SparkContext(appName="S3 Test")

    use_pip_modules(sc)


    sc.stop()
