from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'local_data_processing_dag',
    default_args=default_args,
    description='A simple local data processing workflow',
    schedule_interval=timedelta(days=1),
)

def generate_data(**kwargs):
    data = {
        'A': [1, 2, 3, 4, 5],
        'B': [10, 20, 30, 40, 50],
        'C': [100, 200, 300, 400, 500],
    }
    df = pd.DataFrame(data)
    df.to_csv('/tmp/generated_data.csv', index=False)
    print("Data generated and saved to /tmp/generated_data.csv")

def transform_data(**kwargs):
    df = pd.read_csv('/tmp/generated_data.csv')
    df['D'] = df['A'] + df['B'] + df['C']  # Example transformation: create a new column
    df.to_csv('/tmp/transformed_data.csv', index=False)
    print("Data transformed and saved to /tmp/transformed_data.csv")

def save_data(**kwargs):
    df = pd.read_csv('/tmp/transformed_data.csv')
    df.to_csv('/tmp/saved_data.csv', index=False)
    print("Data saved to /tmp/saved_data.csv")

def load_data(**kwargs):
    df = pd.read_csv('/tmp/saved_data.csv')
    df.to_csv('/tmp/loaded_data.csv', index=False)
    print("Data loaded and saved to /tmp/loaded_data.csv")

def generate_report(**kwargs):
    df = pd.read_csv('/tmp/loaded_data.csv')
    report = df.describe()  # Example report: data summary
    report.to_csv('/tmp/report.csv')
    print("Report generated and saved to /tmp/report.csv")

generate_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

generate_task >> transform_task >> save_task >> load_task >> report_task
