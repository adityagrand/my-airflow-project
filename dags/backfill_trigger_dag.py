from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='backfill_trigger_dag',
         schedule=None,
         start_date=datetime(2022, 1, 1),
         tags=['backfill-trigger-cli'],
         catchup=False) as dag:

    # Use the UI to trigger a DAG run with conf to trigger a backfill, passing in start/end dates and dag_id etc:
    trigger_backfill = BashOperator(
        task_id='trigger_backfill',
        #bash_command="airflow backfill create --dag-id {{ dag_run.conf['dag_id'] }} --from-date {{ dag_run.conf['date_start'] }}  --to-date {{ dag_run.conf['date_end'] }}"
        bash_command=r"""
python -c 'import os
from sqlalchemy.engine import make_url

for key in ("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW__CORE__SQL_ALCHEMY_CONN"):
    value = os.environ.get(key)
    if value is None:
        print(f"{key}: unset")
    elif not value.strip():
        print(f"{key}: set but empty")
    else:
        try:
            make_url(value)
            print(f"{key}: set; URL syntax is valid")
        except Exception:
            print(f"{key}: set; URL syntax is invalid")

print("AIRFLOW_CONFIG:", os.environ.get("AIRFLOW_CONFIG", "not set"))
print("AIRFLOW_HOME:", os.environ.get("AIRFLOW_HOME", "not set"))
'
"""
    )

    trigger_backfill