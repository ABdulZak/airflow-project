from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import airflow

from functions.default import default_arg, send_on_success
from functions.task_2_func import download_data_for_100_codes, clean_data, deploy_to_s3, load_to_postgres

dag = DAG(
    dag_id='task_2_dag',
    default_args=default_arg,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    on_success_callback=send_on_success,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

download_codes = PythonOperator(
    task_id='download_codes',
    python_callable=download_data_for_100_codes,
    dag=dag,
)

clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

deploy_to_s3 =  PythonOperator(
    task_id='deploy_to_s3',
    python_callable=deploy_to_s3,
    dag=dag,
)

load_to_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

start >> download_codes >> clean_data >> deploy_to_s3 >> load_to_postgres
