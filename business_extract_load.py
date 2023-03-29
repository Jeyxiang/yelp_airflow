from datetime import datetime
import requests
import pandas as pd
# from pymongo import MongoClient
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    'owner': 'airflow',
}

#Vairables need to be configured first by going to Airflow webserver > admin > variables > create new variable called BASE_PATH and equal it to folder where your json file is located
#If not using any cloud storage, another way is to store to the JSON file on github and reference the link to read from
BASE_PATH = Variable.get("BASE_PATH")

GOOGLE_CONN_ID = "google_cloud_default"
BUCKET_NAME = 'yelp_dataset_is3107_group37'
DATASET_ID = "etl-dag.yelp_dataset"
BIGQUERY_TABLE_NAME = "yelp_business"
GCS_OBJECT_NAME = "extract_transform_business.csv"
OUT_BUSINESS_PATH = f"{BASE_PATH}/{GCS_OBJECT_NAME}"

with DAG(
    'yelp_dataset_etl_google',
    default_args=default_args,
    description='DAG to extract Yelp Dataset from local directory, perform cleaning and transformation before loading to BigQuery',
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    def extract(**kwargs):
        ti = kwargs['ti']              

        business_df = pd.read_json(f"{BASE_PATH}/yelp_academic_dataset_business.json", lines=True)
        business_df_string = business_df.to_json()
        ti.xcom_push('business_data', business_df_string)
              
        
        
    def load(**kwargs):
        ti = kwargs['ti']

        extract_business_string = ti.xcom_pull(task_ids='extract', key='business_data')
        business_data = json.loads(extract_business_string)      
        business_df = pd.DataFrame(business_data)
        # business_df.to_csv(OUT_BUSINESS_PATH, index=False, header=False)
        print(business_df.head())


    stored_business_data_gcs = LocalFilesystemToGCSOperator(
        task_id="store_business_data_to_gcs",
        gcp_conn_id=GOOGLE_CONN_ID,
        src=OUT_BUSINESS_PATH,
        dst=GCS_OBJECT_NAME,
        bucket=BUCKET_NAME
    )

    loaded_tweets_data_bigquery = GCSToBigQueryOperator(
        task_id='load_tweets_to_bigquery',
        # bigquery_conn_id=GOOGLE_CONN_ID,
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],
        destination_project_dataset_table=f"{DATASET_ID}.{BIGQUERY_TABLE_NAME}",
        schema_fields=[ #based on https://cloud.google.com/bigquery/docs/schemas
            {'name': 'businessId', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},            
            {'name': 'lat', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'long', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'stars', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
            {'name': 'review_count', 'type': 'INT64', 'mode': 'NULLABLE'},
            {'name': 'is_open', 'type': 'BOOL', 'mode': 'NULLABLE'},
            {'name': 'attributes', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'categories', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'hours', 'type': 'STRING', 'mode': 'NULLABLE'},           
        ],
        autodetect=False,
        write_disposition='WRITE_TRUNCATE', #If the table already exists - overwrites the table data
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )


extract_task >> load_task >> stored_business_data_gcs >> loaded_tweets_data_bigquery
    

#### useless code below:
 # extract_reviews_string = ti.xcom_pull(task_ids='extract', key='business_data')
        # reviews_data = json.loads(extract_reviews_string)      
        # reviews_df = pd.DataFrame(reviews_data)
        # print(reviews_df.head())
        # aft this need to convert to CSV then can load to bigquery   
# data = json.load(open(f"{BASE_PATH}/yelp_academic_dataset_review.json", "r"))
        # reviews_df = pd.DataFrame.from_dict(data, orient="index")
        # reviews_df = pd.read_json(f"{BASE_PATH}/yelp_academic_dataset_review.json", orient="records", lines=True, chunksize=5)
        # reviews_df_string = reviews_df.to_json()
        # print(type(reviews_df))
        
        # ti.xcom_push('review_data', reviews_df)