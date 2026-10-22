from airflow.sdk import dag,task,DAG
from pendulum import datetime

@dag(
    schedule="@daily", 
    start_date=datetime(2026, 1, 1),
    description="A simple DAG that prints a message",
    tags=["example", "hello_world"],
    max_consecutive_failed_dag_runs=1,
    )
def my_dag():
    from airflow.operators.python import PythonOperator
    
    def my_task():
        print("Hello, Airflow!")

    task = PythonOperator(
        task_id="my_task",
        python_callable=my_task,
    )

with DAG(
            dag_id="my_dag_without_decorator", 
            default_args={"owner": "airflow"},
            schedule="@daily", 
            start_date=datetime(2026, 1, 1),
            description="A simple DAG that prints a message",
            tags=["example", "hello_world"],
            max_consecutive_failed_dag_runs=1,
            ):

            @task
            def my_task_without_decorator():
                print("Hello, Airflow!")
            
            my_task_without_decorator()

my_dag()            
