from airflow.models import Variable
from sqlalchemy import create_engine
import requests
import pandas as pd
import json

ENGINE = create_engine(Variable.get('dwh_postgres_conn'))

def get_token(response):
    # Function to get the token from the response
    # This token is used to download the file from google drive
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value


def download_data_from_google_drive():
    # Function to download data from google drive
    # 1. Get the file id from the airflow variable
    # 2. Download the file from google drive
    # 3. Save the file in the include folder

    url = 'https://drive.google.com/uc?export=download'
    session = requests.Session()

    response = session.get(url, params={'id': Variable.get('google_drive_file_id')}, stream=True)
    token = get_token(response)

    if token:
        params = {'id': Variable.get('google_drive_file_id'), 'confirm': token}
        response = session.get(url, params=params, stream=True)

    CHUNK_SIZE = 32768
    with open('include/task_1.json', 'wb') as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:
                f.write(chunk)

def clean_data():
    # Function to clean the data
    # For this step jupiter notebook was used to understand the data and clean it
    # it is available in the folders as well


    with open('include/task_1.json', 'r') as f:
        data = json.load(f)
    df = pd.DataFrame(data['data'])


    df_publisher = df[['publisher_id','publisher_name']].drop_duplicates()
    df_publisher.reset_index(drop=True, inplace=True)
    df.drop('publisher_name', axis=1, inplace=True)

    df_tracker = df[['tracking_id','tracker_name']].drop_duplicates()
    df_tracker.reset_index(drop=True, inplace=True)
    df.drop('tracker_name', axis=1, inplace=True)

    df_device = df[['click_user_agent','ios_ifa','ios_ifv','android_id','google_aid','os_name','os_version','device_manufacturer','device_model','device_type','is_bot']].drop_duplicates()
    df_device.reset_index(drop=True, inplace=True)
    df.drop(['ios_ifa','ios_ifv','android_id','google_aid','os_name','os_version','device_manufacturer','device_model','device_type','is_bot'], axis=1, inplace=True)

    df_country = df[['country_iso_code','city']].drop_duplicates()
    df_country.reset_index(drop=True, inplace=True)
    df_country['id'] = df_country.index
    df['location_id'] = df[['country_iso_code', 'city']].apply(
        lambda x: int(df_country[(df_country['country_iso_code'] == x['country_iso_code']) & (df_country['city'] == x['city'])]['id'].values[0]) 
        if not df_country[(df_country['country_iso_code'] == x['country_iso_code']) & (df_country['city'] == x['city'])].empty 
        else -1, 
        axis=1
    )
    df['location_id'] = df['location_id'].astype(int)
    df.drop(['country_iso_code', 'city'], axis=1, inplace=True)


    df.to_csv('include/main_df.csv', index=False)
    df_publisher.to_csv('include/publisher_df.csv', index=False)
    df_tracker.to_csv('include/tracker_df.csv', index=False)
    df_device.to_csv('include/device_df.csv', index=False)
    df_country.to_csv('include/country_df.csv', index=False)


# Function to insert the data into the database
def insert_main():
    df = pd.read_csv('include/main_df.csv')
    df.to_sql('main', ENGINE, if_exists='append', index=False)

def insert_publisher():
    df = pd.read_csv('include/publisher_df.csv')
    df.to_sql('publisher', ENGINE, if_exists='append', index=False)

def insert_tracker():
    df = pd.read_csv('include/tracker_df.csv')
    df.to_sql('tracker', ENGINE, if_exists='append', index=False)

def insert_device():
    df = pd.read_csv('include/device_df.csv')
    df.to_sql('device', ENGINE, if_exists='append', index=False)    

def insert_country():
    df = pd.read_csv('include/country_df.csv')
    df.to_sql('locations', ENGINE, if_exists='append', index=False)


# SQL Executive commands for creating tables in the database (Postgres)
tables = """
CREATE TABLE IF NOT EXISTS main (
    application_id              INTEGER,
    publisher_id                INTEGER,
    tracking_id                 BIGINT,
    click_timestamp             INTEGER,
    click_datetime              TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    click_ipv6                  VARCHAR(255),
    click_url_parameters        VARCHAR(255),
    click_id                    NUMERIC,
    click_user_agent            TEXT,
    location_id                 INTEGER,
    load_dt                     TIMESTAMP        DEFAULT CURRENT_TIMESTAMP
);
  
CREATE TABLE IF NOT EXISTS publisher (
    publisher_id                INTEGER             PRIMARY KEY,
    publisher_name              VARCHAR(255),
    load_dt                     TIMESTAMP        DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE main ADD FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id);

CREATE TABLE IF NOT EXISTS device (
    click_user_agent            TEXT                PRIMARY KEY,
    ios_ifa                     NUMERIC,
    ios_ifv                     NUMERIC,
    android_id                  NUMERIC,
    google_aid                  NUMERIC,
    os_name                     VARCHAR(255),
    os_version                  VARCHAR(255),
    device_model                VARCHAR(255),
    device_manufacturer         VARCHAR(255),
    device_type                 VARCHAR(255),
    is_bot                      BOOLEAN,
    load_dt                     TIMESTAMP        DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE main ADD FOREIGN KEY (click_user_agent) REFERENCES device(click_user_agent);

CREATE TABLE IF NOT EXISTS tracker (
    tracking_id                 BIGINT             PRIMARY KEY,
    tracker_name                VARCHAR(255),
    load_dt                     TIMESTAMP        DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE main ADD FOREIGN KEY (tracking_id) REFERENCES tracker(tracking_id);

CREATE TABLE IF NOT EXISTS locations (
    id                          INTEGER             PRIMARY KEY,
    country_iso_code            VARCHAR(2)          ,
    city                VARCHAR(255)        
);

ALTER TABLE main ADD FOREIGN KEY (location_id) REFERENCES locations(id);

TRUNCATE main,publisher,device,tracker,locations RESTART IDENTITY;

"""
