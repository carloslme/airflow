from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'carloslme',
    'depends_on_past': False,
    'start_date': datetime(2021,2,9),
    'email': ['carloslme@ivoy.mx'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# define the DAG
dag = DAG(
    'task_01',
    description='Basic Bash and Python Operators',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
)

# define the first task
hello_bash = BashOperator(
    task_id='Hello_Bash',
    bash_command='echo "Hello Airflow from bash"',
    dag=dag
)

def say_hello():
    print("Hello Airflow from Python")

# define the second task
hello_python = BashOperator(
    task_id='Hello_Python',
    python_callable=say_hello,
    dag=dag
)

hello_bash >> hello_python