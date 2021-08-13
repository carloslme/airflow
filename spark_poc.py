from random import randint  # Import to generate random numbers
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG("my_dag",  # Dag id
          # start date, the 1st of January 2021
          start_date=datetime(2021, 1, 1),
          # Cron expression, here it is a preset of Airflow, @daily means once every day.
          schedule_interval='@daily',
          catchup=False  # Catchup
          )


def _training_model():
    return randint(1, 10)  # return an integer between 1 - 10


# Tasks are implemented under the dag object
training_model_tasks = [
    PythonOperator(
        task_id=f"training_model_{model_id}",
        python_callable=_training_model,
        op_kwargs={
            "model": model_id
        }
    ) for model_id in ['A', 'B', 'C']
]


def _choosing_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
    'training_model_A',
    'training_model_B',
    'training_model_C'
    ])
    if max(accuracies) > 8:
        return 'accurate'
    else:
        return 'inaccurate'
    with DAG(...) as dag:
    choosing_best_model = BranchPythonOperator(
    task_id="choosing_best_model",
    python_callable=_choosing_best_model
    )