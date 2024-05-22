from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def task1_function():
    print("Running Task 1")

def task2_function():
    print("Running Task 2")

def task3_function():
    print("Running Task 3")

def task4_function():
    print("Running Task 4")

def task5_function():
    print("Running Task 5")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'example_dag_with_functions',
    default_args=default_args,
    description='A simple DAG with 5 tasks, each running a different function',
    schedule_interval='@daily',
)

# Define the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

task1 = PythonOperator(
    task_id='task1',
    python_callable=task1_function,
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2_function,
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=task3_function,
    dag=dag,
)

task4 = PythonOperator(
    task_id='task4',
    python_callable=task4_function,
    dag=dag,
)

task5 = PythonOperator(
    task_id='task5',
    python_callable=task5_function,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> task1 >> task2 >> task3 >> task4 >> task5 >> end
