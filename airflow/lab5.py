from __future__ import print_function
import airflow
import pytz
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.models import Variable

start_date = datetime(2019, 02, 06, 0, 0, 0, tzinfo=pytz.utc)

os.environ['SPARK_HOME'] = '/home/ubuntu/spark-2.3.1-bin-hadoop2.7/'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'schedule_interval': None,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=45)

}

dag = DAG('lab5',
          description = 'Airflow simple',
          schedule_interval = None,
          default_args = default_args)


spark_task = BashOperator(
    task_id='spark_python',
    bash_command='/home/ubuntu/spark-2.3.1-bin-hadoop2.7/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.3 /home/ubuntu/pyspark_read_s3/main.py',
    dag = dag
    )
    
    

load_to_hive = HiveOperator(
  hive_cli_conn_id = 'hive_cli_default',
  task_id = 'load_to_hive',
  hql = 'SELECT B_I from finaldata',
  dag = dag
)

spark_task >> load_to_hive

