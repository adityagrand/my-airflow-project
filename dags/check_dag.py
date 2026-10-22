from airflow.sdk import dag,task,DAG
from pendulum import datetime

with DAG(
            dag_id="check_dag", 
            default_args={"owner": "airflow"},
            schedule="0 0 * * *", 
            start_date=datetime(2026, 1, 1),
            description="DAG to check data",
            max_consecutive_failed_dag_runs=1,
            ):

            @task.bash
            def create_file():
                return "echo 'Hi there!' > /tmp/dummy.txt"
            
            @task.bash
            def check_file():
                return "test -f /tmp/dummy.txt && echo 'File exists' || echo 'File does not exist'"

            @task
            def read_file():
                with open("/tmp/dummy.txt", "rb") as f:
                    content = f.read()
                print(f"File content: {content}")
            
            create_file() >> check_file() >> read_file()

                