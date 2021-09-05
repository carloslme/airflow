# Dag Scheduler for Titanic data
from airflow.utils.dates import days_ago

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

default_args = {
    'owner': 'carloslme',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['carloslme@ivoy.mx'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'task_02',
    description='Data extraction from internet and calculate average',
    default_args=default_args
)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/Mineria/Titanic/master/csv/train.csv -o ~/train.csv',
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/train.csv')
    avg = df.Age.mean()
    return avg

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calculate_average_age')
    print(f"The average age was {value} years old.")

task_average_age = PythonOperator(
    task_id = 'calculate_average_age',
    python_callable=calculate_mean_age,
    dag=dag
)

task_print_age = PythonOperator(
    task_id='show_age',
    python_callable=print_age,
    dag=dag
)

get_data >> task_average_age >> task_print_age