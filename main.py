from pyspark import SparkContext

#import findspark

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# import configparser

import argparse

def use_pip_modules(spark_context):
    # import configparser

    # config = configparser.ConfigParser()
    # config.read(os.path.expanduser("~/.aws/credentials"))
    # access_id = config.get("default", "aws_access_key_id") 
    # access_key = config.get("default", "aws_secret_access_key")

    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", "AKIAJCUQRJ3N3FKGD6EA")
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", "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")

    sqlContext = SQLContext(spark_context)

    df = sqlContext.read.json("s3n://bigdata-testing123/ethPrice_3.json")
    df.show()



if __name__ == '__main__': 
    # reading arguments
    # findspark.init()
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--pip_modules', required=False, help='pip modules zip path')
    # args = parser.parse_args()


    # config = configparser.ConfigParser()
    # config.read(os.path.expanduser("~/.aws/credentials"))
    # access_id = config.get("default", "aws_access_key_id") 
    # access_key = config.get("default", "aws_secret_access_key")

    sc = SparkContext(appName="S3 Test")

    # hadoop_conf=sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    # hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
    # hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)
    use_pip_modules(sc)

    # df = spark.read.json("s3n://bigdata-testing123/topics/confluent-out-prices/partition=0/*.json")
    # df.show()

    sc.stop()
