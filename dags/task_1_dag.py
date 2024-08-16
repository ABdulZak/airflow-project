from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from functions.task_1_func import download_data_from_google_drive, clean_data, tables, insert_main, insert_publisher, insert_tracker, insert_device, insert_country

import airflow


dag = DAG(
    dag_id = 'task_1_dag',
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = None,
)

start = DummyOperator(
    task_id = 'start',
    dag = dag,
)

create_table = PostgresOperator(
    task_id = 'create_table',
    sql = tables,
    postgres_conn_id = 'dwh_conn',
    dag = dag,
)

download_data = PythonOperator(
    task_id = 'download_data',
    python_callable = download_data_from_google_drive,
    dag = dag,
)

clean_data = PythonOperator(
    task_id = 'clean_data',
    python_callable = clean_data,
    dag = dag,
)

insert_main = PythonOperator(
    task_id = 'insert_main',
    python_callable = insert_main,
    dag = dag,
)

insert_publisher = PythonOperator(
    task_id = 'insert_publisher',
    python_callable = insert_publisher,
    dag = dag,
)

insert_tracker = PythonOperator(
    task_id = 'insert_tracker',
    python_callable = insert_tracker,
    dag = dag,
)

insert_device = PythonOperator(
    task_id = 'insert_device',
    python_callable = insert_device,
    dag = dag,
)

insert_country = PythonOperator(
    task_id = 'insert_country',
    python_callable = insert_country,
    dag = dag,
)

end = DummyOperator(
    task_id = 'end',
    dag = dag,
)

start >> create_table >> download_data >> clean_data >> [insert_main, insert_publisher, insert_tracker, insert_device, insert_country] >> end 
