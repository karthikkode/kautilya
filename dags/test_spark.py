from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# 1. Define default arguments
default_args = {
    'owner': 'kautilya',
    'start_date': datetime(2023, 1, 1),
}

# 2. Define the DAG object (This is what Airflow looks for!)
with DAG('01_kautilya_test', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    
    # 3. Define the Task
    test_spark_job = SparkSubmitOperator(
        task_id='test_spark_connection',
        # We keep the connection for auth/settings, but force the master URL here
        conn_id='spark_default', 
        application='/opt/airflow/scripts/pi.py',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='512m',
        name='kautilya_test_job',
        verbose=True,
        # FORCE the master URL here to kill the YARN error
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.host": "gdelt-airflow-scheduler",
            "spark.driver.port": "20020",
            "spark.blockManager.port": "20021"
        }
    )