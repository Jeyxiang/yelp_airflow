from datetime import datetime
import requests
import pandas as pd
# from pymongo import MongoClient
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from google.cloud import bigquery
import os

from airflow.providers.google.cloud.operators.bigquery import (BigQueryUpdateTableOperator,
                                                               BigQueryCreateEmptyDatasetOperator,
                                                               BigQueryGetDatasetTablesOperator)


default_args = {
    'owner': 'airflow',
}

#Vairables need to be configured first by going to Airflow webserver > admin > variables > create new variable called BASE_PATH and equal it to folder where your json file is located
#If not using any cloud storage, another way is to store to the JSON file on github and reference the link to read from. 

BASE_PATH = Variable.get("BASE_PATH") # stored from os.path.abspath(os.curdir)
CREDENTIALS = Variable.get("CREDENTIALS") # Path to JSON key object

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS # Google Auth

# CONFIG VARIABLES
GOOGLE_CONN_ID = "gcp_conn"
PROJECT_ID = "yelp-data-warehouse"
DATASET_ID = "yelp_dataset"
BUSINESS_TABLE_ID = "yelp_business"
NAMES_TABLE_ID = "yelp_names"


with DAG(
    'yelp_dataset_etl',
    default_args=default_args,
    description='DAG to extract Yelp Dataset from local directory, perform cleaning and transformation before loading to BigQuery',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    def extract_business(**kwargs):
        ti = kwargs['ti']         
        business_df = pd.read_json(f"{BASE_PATH}/yelp_academic_dataset_business.json", lines=True)
        business_df = business_df[['business_id',"name","city","state","stars","review_count","categories"]]
        business_df_string = business_df.to_json()
        ti.xcom_push('business_data', business_df_string)
              
        
    def filter_business(**kwargs):
        ti = kwargs['ti']

        extract_business_string = ti.xcom_pull(task_ids='extract_business', key='business_data')
        business_data = json.loads(extract_business_string)      
        business_df = pd.DataFrame(business_data)
        food_df  = business_df[(business_df['categories'].str.contains(pat = 'Food', regex = True)) 
                             | (business_df['categories'].str.contains(pat = 'Restaurants', regex = True))]
        food_json_string = food_df.to_json(orient="records")
        ti.xcom_push('filtered_business_data', food_json_string)

    def group_business(**kwargs):
        ti = kwargs['ti']
        extract_filtered_business_string = ti.xcom_pull(task_ids='filter_business', key='filtered_business_data')
        filtered_business_data = json.loads(extract_filtered_business_string)
        filtered_business_df = pd.DataFrame(filtered_business_data)
        name_df = filtered_business_df.groupby(['name']).agg({'stars' : 'mean', 'review_count' : 'sum', 'name' : 'count'}
                                            ).rename(columns = {'stars' : 'avg_review','name' : 'total_outlets'}
                                                     ).reset_index()
        name_json_string = name_df.to_json(orient="records")
        ti.xcom_push('name_data', name_json_string)

    def load_food_to_bq(**kwargs):
        ti = kwargs['ti']
        extract_filtered_business_string = ti.xcom_pull(task_ids='filter_business', key='filtered_business_data')
        filtered_business_data = json.loads(extract_filtered_business_string) 
        bqclient = bigquery.Client(project = PROJECT_ID)    
        dataset  = bqclient.dataset(DATASET_ID)
        table = dataset.table(BUSINESS_TABLE_ID)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = bqclient.load_table_from_json(filtered_business_data, table, job_config=job_config)
        # ERROR HANDLING TODO
        result = job.result()

    def load_names_to_bq(**kwargs):
        ti = kwargs['ti']
        grouped_business_string = ti.xcom_pull(task_ids='group_business', key='name_data')
        grouped_business_data = json.loads(grouped_business_string) 
        bqclient = bigquery.Client(project = PROJECT_ID)    
        dataset  = bqclient.dataset(DATASET_ID)
        table = dataset.table(NAMES_TABLE_ID)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = bqclient.load_table_from_json(grouped_business_data, table,job_config=job_config)
        # ERROR HANDLING TODO
        result = job.result()



    # Python Operators
    
    extract_business_task = PythonOperator(
        task_id='extract_business',
        python_callable=extract_business,
    )

    filtered_business_task = PythonOperator(
        task_id='filter_business',
        python_callable=filter_business,
    )

    group_business_task = PythonOperator(
        task_id='group_business',
        python_callable=group_business,
    )

    load_filtered_bq = PythonOperator(
        task_id='load_food_to_bq',
        python_callable=load_food_to_bq,
    )

    load_grouped_business_bq = PythonOperator(
        task_id='load_names_to_bq',
        python_callable=load_names_to_bq,
    )

extract_business_task >> filtered_business_task >> load_filtered_bq
filtered_business_task >> group_business_task >> load_grouped_business_bq