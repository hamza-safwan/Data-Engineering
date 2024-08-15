from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval=timedelta(days=1),
)

def extract():
    # Read input CSV file
    df = pd.read_csv('/path/to/data/input.csv')
    df.to_csv('/path/to/data/temp.csv', index=False)

def transform():
    # Load the data
    df = pd.read_csv('/path/to/data/temp.csv')

    # Filter data: Select rows where age > 30
    df = df[df['age'] > 30]

    # Aggregate data: Calculate the average salary by department
    avg_salary = df.groupby('department')['salary'].mean().reset_index()

    # Save the transformed data
    avg_salary.to_csv('/path/to/data/output.csv', index=False)

def load():
    # Here you can load the data to your target destination
    # For this example, we will just print the transformed data
    df = pd.read_csv('/path/to/data/output.csv')
    print(df)

# Define tasks
t1 = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# Set task dependencies
t1 >> t2 >> t3
