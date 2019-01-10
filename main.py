from os.path import abspath, join

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse

def use_pip_modules(spark_session):

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
    
    spark_session.sql("CREATE TABLE IF NOT EXISTS finaldata (DT STRING, SHOP_HOUR STRING, BASKET_ID STRING, CUST_CODE STRING, STORE_CODE STRING, PROD_CODE STRING, QUANTITY INT, SPEND FLOAT, YY STRING, MM STRING, DD STRING) STORED AS PARQUET")

    df = spark_session.read.format("csv").option("header", "true").schema(schema).load("/home/ubuntu/the-test-poc/supermarket_data.csv")
    
    df.show()

    print(df.count())
    
    # Drop duplicate rows

    full_removed = df.dropDuplicates()

    # Remove the row that have null value in any columns

    missing_removed = full_removed.na.drop()

    print(missing_removed.count())

    # create temporary view

    missing_removed.createOrReplaceTempView("transaction_record")

    final_datadf = spark_session.sql("SELECT from_unixtime(unix_timestamp(SHOP_DATE,'yyyyMMdd'),'yyyy-MM-dd') AS DT, SHOP_HOUR, BASKET_ID, CUST_CODE, STORE_CODE, PROD_CODE, QUANTITY, SPEND FROM transaction_record")

    final_datadf = final_datadf.withColumn('YY', F.split(final_datadf['DT'], '-')[0])
    final_datadf = final_datadf.withColumn('MM', F.split(final_datadf['DT'], '-')[1])
    final_datadf = final_datadf.withColumn('DD', F.split(final_datadf['DT'], '-')[2])

    final_datadf.show()

    final_datadf.cache()

    final_datadf.write.mode("overwrite").insertInto("finaldata")

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

    use_pip_modules(spark)
