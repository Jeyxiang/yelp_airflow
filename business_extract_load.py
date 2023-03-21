from datetime import datetime
import requests
import pandas as pd
# from pymongo import MongoClient
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable

default_args = {
    'owner': 'airflow',
}

#Vairables need to be configured first by going to Airflow webserver > admin > variables > create new variable called BASE_PATH and equal it to folder where your json file is located
#If not using any cloud storage, another way is to store to the JSON file on github and reference the link to read from
BASE_PATH = Variable.get("BASE_PATH")

with DAG(
    'yelp_dataset_etl',
    default_args=default_args,
    description='DAG to extract Yelp Dataset from local directory, perform cleaning and transformation before loading to BigQuery',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    def extract(**kwargs):
        ti = kwargs['ti']              

        business_df = pd.read_json(f"{BASE_PATH}/yelp_academic_dataset_business.json", lines=True)
        business_df_string = business_df.to_json()
        ti.xcom_push('business_data', business_df_string)

        # data = json.load(open(f"{BASE_PATH}/yelp_academic_dataset_review.json", "r"))
        # reviews_df = pd.DataFrame.from_dict(data, orient="index")
        # reviews_df = pd.read_json(f"{BASE_PATH}/yelp_academic_dataset_review.json", orient="records", lines=True, chunksize=5)
        # reviews_df_string = reviews_df.to_json()
        # print(type(reviews_df))
        
        # ti.xcom_push('review_data', reviews_df)
        # can do transform here or def a new fx to transform
              
        
        
    def load(**kwargs):
        ti = kwargs['ti']

        extract_business_string = ti.xcom_pull(task_ids='extract', key='business_data')
        business_data = json.loads(extract_business_string)      
        business_df = pd.DataFrame(business_data)
        print(business_df.head())

        # extract_reviews_string = ti.xcom_pull(task_ids='extract', key='business_data')
        # reviews_data = json.loads(extract_reviews_string)      
        # reviews_df = pd.DataFrame(reviews_data)
        # print(reviews_df.head())
        # aft this need to convert to CSV then can load to bigquery

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

extract_task >> load_task
    