import requests
import pandas as pd
import json
from sqlalchemy import create_engine
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from io import StringIO

ENGINE = create_engine(Variable.get('dwh_postgres_conn'))

def download_data_for_100_codes():
    url = Variable.get('Open_Food_Facts_Api')

    params = {
        'page':1,
        'page_size':100,
    }

    response = requests.get(url, params=params)

    data = response.json()

    df = pd.read_json(json.dumps(data['products']))

    df = df[['code','creator','created_t','last_modified_t','product_name','generic_name','quantity','packaging','brands','categories','origins','manufacturing_places','labels','emb_codes','stores','countries','ingredients_text','allergens','nutriments','additives_n','ingredients_from_palm_oil_n','nutrition_grades']]

    df.to_csv('include/products.csv', index=False)


def clean_data():
    df = pd.read_csv('include/products.csv')

    df['created_t'] = pd.to_datetime(df['created_t'], unit='s')
    df['last_modified_t'] = pd.to_datetime(df['last_modified_t'], unit='s')

    df.dropna(inplace=True)

    df.to_csv('include/cleaned_products.csv', index=False)

def deploy_to_s3():
    hook = S3Hook(aws_conn_id='s3_conn')

    hook.load_file(
        filename='include/cleaned_products.csv',
        key='cleaned_products.csv',
        bucket_name='korzinka-test-task',
        replace=True
    )

def load_to_postgres():
    hook = S3Hook(aws_conn_id='s3_conn')

    pg_hook = PostgresHook(postgres_conn_id='dwh_conn')  

    s3_key = 'cleaned_products.csv'
    bucket = 'korzinka-test-task'

    file_obj = hook.get_key(s3_key, bucket_name=bucket)  
    file_content = file_obj.get()['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(file_content))
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    csv_buffer.seek(0)

    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.copy_expert(f"COPY products FROM STDIN WITH CSV HEADER", csv_buffer)
    connection.commit()
    cursor.close()