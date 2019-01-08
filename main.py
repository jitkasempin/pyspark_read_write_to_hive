from os.path import abspath, join

from pyspark import SparkContext
#import findspark
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# import configparser
import argparse

def use_pip_modules(spark_session):

    #hadoop_conf = spark_context._jsc.hadoopConfiguration()
    #hadoop_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #hadoop_conf.set("spark.hadoop.fs.s3a.awsAccessKeyId", "AKIAIVJZJEHE4ATMTPJQ")
    #hadoop_conf.set("spark.hadoop.fs.s3a.awsSecretAccessKey", "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")
    #sqlContext = SQLContext(spark_context)

    spark_session.sparkContext.setSystemProperty("hive.metastore.uris", "thrift://localhost:9083")
    
    schema = StructType([
        StructField("SHOP_DATE",StringType(),True),
        StructField("SHOP_HOUR",StringType(),True),
        StructField("BASKET_ID",StringType(),True),
        StructField("CUST_CODE",StringType(),True),
        StructField("STORE_CODE",StringType(),True),
        StructField("PROD_CODE",StringType(),True),
        StructField("QUANTITY",IntegerType(),True),
        StructField("SPEND",FloatType(),True)
    ])

    #spark_session.sql("CREATE TABLE IF NOT EXISTS transactionals (basket_id STRING, cust_code STRING, store_code STRING, prod_code STRING, quantity INT, spend FLOAT) PARTITIONED BY (shop_date STRING, shop_hour STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    
    spark_session.sql("CREATE TABLE IF NOT EXISTS datatest (DT STRING, SHOP_HOUR STRING, BASKET_ID STRING, CUST_CODE STRING, STORE_CODE STRING, PROD_CODE STRING, QUANTITY INT, SPEND FLOAT) STORED AS PARQUET")

    df = spark_session.read.format("csv").option("header", "true").schema(schema).load("/home/ubuntu/the-test-poc/supermarket_data.csv")
    
    df.show()

    print(df.count())

    full_removed = df.dropDuplicates()

    # print(full_removed.count())

    missing_removed = full_removed.na.drop()

    print(missing_removed.count())

    # create temporary view

    missing_removed.createOrReplaceTempView("transaction_record")

    final_datadf = spark_session.sql("SELECT from_unixtime(unix_timestamp(SHOP_DATE,'yyyyMMdd'),'yyyy-MM-dd') AS DT, SHOP_HOUR, BASKET_ID, CUST_CODE, STORE_CODE, PROD_CODE, QUANTITY, SPEND FROM transaction_record")

    final_datadf = final_datadf.withColumn('YY', F.split(final_datadf['DT'], '-')[0])
    final_datadf = final_datadf.withColumn('MM', F.split(final_datadf['DT'], '-')[1])
    final_datadf = final_datadf.withColumn('DD', F.split(final_datadf['DT'], '-')[2])

    final_datadf.show()

    #spark_session.sql("INSERT INTO TABLE dataraws PARTITION (STORE_CODE) SELECT SHOP_DATE, SHOP_HOUR, BASKET_ID, CUST_CODE, PROD_CODE, QUANTITY, SPEND, STORE_CODE FROM transaction_record")

    final_datadf.cache()

    final_datadf.write.mode("overwrite").insertInto("datatest")

    spark_session.stop()



if __name__ == '__main__': 

    warehouse_location = abspath('spark-warehouse')

    spark = SparkSession \
        .builder \
        .appName("Simple") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    #sc = SparkContext(appName="S3 Test")

    use_pip_modules(spark)

    #sc.stop()
