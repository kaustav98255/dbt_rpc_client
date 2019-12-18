import os.path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from dags.src.dbtRPCClientRunner import DBTRPCClient

AIRFLOW_DAGS = os.environ['AIRFLOW_DAGS']

# Unique ID for DAG job
WORKFLOW_DAG_ID = 'dag_dbt'

# start/end times are datetime objects
# here we start execution on 31st Aug 2019
WORKFLOW_START_DATE = datetime(2019, 10, 31)

# schedule/retry intervals are timedelta objects
# here we execute the DAGs tasks at 06:25 UTC every morning
WORKFLOW_SCHEDULE_INTERVAL = '25 7 * * *'

# arguments are applied by default to all tasks in the DAG
WORKFLOW_DEFAULT_ARGS = {
    'owner': 'kaustav.mitra',
    'start_date': WORKFLOW_START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

# initialize the DAG
dag = DAG(
    dag_id=WORKFLOW_DAG_ID,
    start_date=WORKFLOW_START_DATE,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
)


def run_dbt(task=None, models=None, exclude=None, **kwargs):

    dbt_client = DBTRPCClient()
    dbt_client.execute(task=task, models=models, exclude=exclude)

    return None


Start = BashOperator(
    task_id='Starting_Message',
    bash_command='echo Starting dbt run',
    dag=dag,
    trigger_rule='all_done'
)

run_dbt_tests = PythonOperator(
    task_id='run_dbt_tests',
    python_callable=run_dbt,
    dag=dag,
    op_kwargs={'task': 'test', 'models': '', 'exclude': ''},
    trigger_rule='all_done'
)

create_dbt_models = PythonOperator(
    task_id='create_dbt_models',
    python_callable=run_dbt,
    dag=dag,
    op_kwargs={'task': 'run', 'models': '', 'exclude': ''},
    trigger_rule='all_done'
)

create_dbt_docs = PythonOperator(
    task_id='create_dbt_docs',
    python_callable=run_dbt,
    dag=dag,
    op_kwargs={'task': 'docs.generate', 'models': '', 'exclude': ''},
    trigger_rule='all_done'
)

Finished = BashOperator(
    task_id='Finished_Message',
    bash_command='echo Finished!',
    dag=dag,
    trigger_rule='all_done'
)

Start >> run_dbt_tests >> create_dbt_models >> create_dbt_docs >> Finished
