# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# default_args to be passed into the DAG constructor - replace with your own
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['carloslmescom@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# define the DAG
# only the first parameter (name) is required
dag = DAG(
        "pyop_example",
        default_args = default_args,
        description = "A simple example with PythonOperators",
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=['example']
)

from datetime import datetime

def print_date_time():
  print("Now it is {}!".format(datetime.now()))

print_date = PythonOperator(
                  task_id="print_date",
                  python_callable=print_date_time,
                  dag=dag)

# sleep function
def print_sleep():
  print("This is sleeping...")

sleep = PythonOperator(
                task_id="sleep",
                python_callable=print_sleep,
                dag=dag)
                
# templated
def print_templated():
  print("this is a template!")

templated = PythonOperator(
                task_id="templated",
                python_callable=print_templated,
                dag=dag)

print_date >> [sleep, templated]
# which is the same as
# print_date.set_downstream(sleep)
# print_date.set_downstream(templated)