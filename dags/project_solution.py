
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Створення DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'project_solution',
    default_args=default_args,
    description='A simple data pipeline',
    schedule_interval='@daily',
    tags=['vvd']
)

# Завдання для виконання скриптів
task1 = SparkSubmitOperator(
    application='dags/vvd/landing_to_bronze.py',
    task_id='landing_to_bronze',
    conn_id='spark_default', 
    verbose=1, 
    conf={'spark.master': 'local'},
    dag=dag,
)

task2 = SparkSubmitOperator(
    application='dags/vvd/bronze_to_silver.py',
    task_id='bronze_to_silver',
    conn_id='spark_default', 
    verbose=1, 
    conf={'spark.master': 'local'},
    dag=dag,
)

task3 = SparkSubmitOperator(
    application='dags/vvd/silver_to_gold.py',
    task_id='silver_to_gold',
    conn_id='spark_default', 
    verbose=1, 
    conf={'spark.master': 'local'},
    dag=dag,
)

# Послідовність виконання завдань
task1 >> task2 >> task3
