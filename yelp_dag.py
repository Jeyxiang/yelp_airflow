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

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS

# CONFIG VARIABLES
GOOGLE_CONN_ID = "gcp_conn"
PROJECT_ID = "yelp-data-warehouse"
DATASET_ID = "yelp_dataset"
BUSINESS_TABLE_ID = "yelp_business"


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
        business_df = business_df[['business_id',"name","city","state","stars","review_count","categories","attributes",
                                   "latitude","longitude"]]
        # drop row with null values (selected colunmns)
        business_df = business_df.dropna(subset = ['business_id','stars','review_count','city','state','categories'])
        business_df_string = business_df.to_json()
        ti.xcom_push('business_data', business_df_string)
              
        
    def filter_business(**kwargs):
        '''
        Filtering out food establishments
        '''
        ti = kwargs['ti']   
        extract_business_string = ti.xcom_pull(task_ids='extract_business', key='business_data')
        business_data = json.loads(extract_business_string)      
        business_df = pd.DataFrame(business_data)
        business_df['categories'] = business_df['categories'].str.lower()
        food_df  = business_df[(business_df['categories'].str.contains(pat = 'food', regex = True)) 
                             | (business_df['categories'].str.contains(pat = 'restaurants', regex = True))]
        food_json_string = food_df.to_json(orient="records")
        ti.xcom_push('filtered_business_data', food_json_string)


    def flatten_business(**kwargs):
        '''
        Flatten nested data by extracting useful information only.
        Fields extracted from attributes: RestaurantsPriceRange2, RestaurantsReservations,RestaurantsDelivery
        '''
        ti = kwargs['ti']
        extract_filtered_business_string = ti.xcom_pull(task_ids='filter_business', key='filtered_business_data')
        filtered_business_data = json.loads(extract_filtered_business_string)
        filtered_business_df = pd.DataFrame(filtered_business_data)

        # new columns
        priceRange = []
        hasReservations = []
        hasDelivery = []

        for idx,business in filtered_business_df.iterrows():
            attribute = business['attributes']
            if attribute is not None:
                if 'RestaurantsPriceRange2' in attribute and attribute['RestaurantsPriceRange2'] is not None:
                    priceRange.append(attribute['RestaurantsPriceRange2'])
                else:
                    priceRange.append('None')
                if 'RestaurantsReservations' in attribute:
                    value = True if attribute['RestaurantsReservations'] == "True" else False
                    hasReservations.append(value)
                else:
                    hasReservations.append(False)
                if 'RestaurantsDelivery' in attribute:
                    value = True if attribute['RestaurantsDelivery'] == "True" else False
                    hasDelivery.append(value)
                else:
                    hasDelivery.append(False)
            else:
                priceRange.append('None')
                hasReservations.append(False)
                hasDelivery.append(False)

        filtered_business_df['price_range'] = priceRange
        filtered_business_df['has_reservation'] = hasReservations
        filtered_business_df['has_delivery'] = hasDelivery
        filtered_business_df = filtered_business_df.drop(columns = ['attributes'])
        business_json_string = filtered_business_df.to_json(orient="records")
        ti.xcom_push('flatten_business_data', business_json_string)


    def group_business(**kwargs):
        '''
        Group businesses by names, aggregate fields
        '''
        ti = kwargs['ti']
        extract_filtered_business_string = ti.xcom_pull(task_ids='flatten_business', key='flatten_business_data')
        filtered_business_data = json.loads(extract_filtered_business_string)
        filtered_business_df = pd.DataFrame(filtered_business_data)
        name_df = filtered_business_df.groupby(['name']
                    ).agg({'stars' : 'mean', 'review_count' : 'sum', 'name' : 'count',
                    'has_reservation' : lambda x: (x == True).sum(), 'has_delivery' : lambda x: (x == True).sum()}
                    ).rename(columns = {'stars' : 'avg_review','name' : 'total_outlets',
                    'has_reservation' : 'accept_reservations', 'has_delivery' : 'offer_delivery'}
                    ).reset_index()
        name_json_string = name_df.to_json(orient="records")
        ti.xcom_push('name_data', name_json_string)


    def count_categories(**kwargs):
        '''
        Count the frequency of each categories
        '''
        ti = kwargs['ti']
        extract_filtered_business_string = ti.xcom_pull(task_ids='filter_business', key='filtered_business_data')
        filtered_business_data = json.loads(extract_filtered_business_string)
        filtered_business_df = pd.DataFrame(filtered_business_data)
        cat_df = filtered_business_df['categories'].apply(lambda x: pd.value_counts(x.split(", "))).sum(axis = 0).to_frame()
        cat_df = cat_df.reset_index()
        cat_df.columns = ['category','count']
        cat_df = cat_df[(cat_df.category != 'food') & (cat_df.category != 'restaurants')]
        cat_json_string = cat_df.to_json(orient="records")
        ti.xcom_push('cat_data', cat_json_string)


    def load_business_to_bq(**kwargs):
        ti = kwargs['ti']
        extract_flatten_business_string = ti.xcom_pull(task_ids='flatten_business', key='flatten_business_data')
        flatten_business_data = json.loads(extract_flatten_business_string)
        try:
            bqclient = bigquery.Client(project = PROJECT_ID)    
            dataset  = bqclient.dataset(DATASET_ID)
            table = dataset.table(BUSINESS_TABLE_ID)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = bqclient.load_table_from_json(flatten_business_data, table, job_config=job_config)
            result = job.result()
        except Exception as e:
            print(e)


    def load_names_to_bq(**kwargs):
        ti = kwargs['ti']
        cat_json_string = ti.xcom_pull(task_ids='group_business', key='name_data')
        cat_data = json.loads(cat_json_string) 
        try:
            bqclient = bigquery.Client(project = PROJECT_ID)    
            dataset  = bqclient.dataset(DATASET_ID)
            table = dataset.table("yelp_names")
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = bqclient.load_table_from_json(cat_data, table,job_config=job_config)
            print(job.result())
        except Exception as e:
            print(e)


    def load_category_to_bq(**kwargs):
        ti = kwargs['ti']
        grouped_business_string = ti.xcom_pull(task_ids='count_categories', key='cat_data')
        grouped_business_data = json.loads(grouped_business_string)
        try:
            bqclient = bigquery.Client(project = PROJECT_ID)    
            dataset  = bqclient.dataset(DATASET_ID)
            table = dataset.table("yelp_categories")
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = bqclient.load_table_from_json(grouped_business_data, table,job_config=job_config)
            result = job.result()
        except Exception as e:
            print(e)



    # Python Operators
    
    extract_business_task = PythonOperator(
        task_id='extract_business',
        python_callable=extract_business,
    )

    filtered_business_task = PythonOperator(
        task_id='filter_business',
        python_callable=filter_business,
    )

    flatten_business_task = PythonOperator(
        task_id='flatten_business',
        python_callable=flatten_business,
    )

    count_categories_task = PythonOperator(
        task_id='count_categories',
        python_callable=count_categories,
    )

    group_business_task = PythonOperator(
        task_id='group_business',
        python_callable=group_business,
    )

    load_business_bq = PythonOperator(
        task_id='load_business_to_bq',
        python_callable=load_business_to_bq,
    )

    load_grouped_business_bq = PythonOperator(
        task_id='load_names_to_bq',
        python_callable=load_names_to_bq,
    )

    load_categories_bq = PythonOperator(
        task_id='load_cat_to_bq',
        python_callable=load_category_to_bq,
    )


extract_business_task >> filtered_business_task >> flatten_business_task >> load_business_bq
flatten_business_task >> group_business_task >> load_grouped_business_bq
filtered_business_task >> count_categories_task >> load_categories_bq