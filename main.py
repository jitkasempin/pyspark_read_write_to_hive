from pyspark import SparkContext
#import findspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# import configparser
import argparse

def use_pip_modules(spark_context):

    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", "your_aws_access_id")
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", "your_aws_access_key")

    sqlContext = SQLContext(spark_context)
    
    schema = StructType([
        StructField("ethPri",FloatType(),True),
        StructField("timeRecord",StringType(),True)
    ])

    df = sqlContext.read.json("s3n://bigdata-testing123/ethPrice_3.json", schema=schema)
    
    df.show()

    df.write.format("jdbc").option("url","jdbc:mysql://jitkasem-mysql.cmmw1yeaonrt.us-east-1.rds.amazonaws.com:3306/jitkasem?user=jitkasem&password=jitkasem341").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "eth_table").mode("overwrite").save()


if __name__ == '__main__': 

    sc = SparkContext(appName="S3 Test")

    use_pip_modules(sc)

    sc.stop()
