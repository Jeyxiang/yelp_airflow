from datetime import datetime
import requests
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from google.cloud import bigquery
import hashlib
import os

from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.bigquery import (BigQueryUpdateTableOperator,
                                                               BigQueryCreateEmptyDatasetOperator,
                                                               BigQueryGetDatasetTablesOperator)



'''
Configure variables via Airflow webserver > admin > variables.
'''

BASE_PATH = Variable.get("BASE_PATH") # relative path of where data is stored
CREDENTIALS = Variable.get("CREDENTIALS") # Path to Service Account JSON key object

# CONFIG VARIABLES
BUCKET_NAME = "is3107_yelp_dataset_etl"
PROJECT_ID = "yelp-data-warehouse"
DATASET_ID = "yelp_dataset"
BUSINESS_TABLE_ID = "yelp_business"
CAT_TABLE_ID = "yelp_categories" 
FRANCHISE_TABLE_ID = "yelp_franchise"
TIPS_TABLE_ID = "yelp_tips"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS
default_args = {
    'owner': 'airflow',
}

with DAG(
    'yelp_dataset_etl',
    default_args=default_args,
    description='DAG to extract Yelp Dataset from GCS, perform cleaning and transformation before loading to BigQuery',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    
    extract_business_from_GCS = GCSToLocalFilesystemOperator(
        task_id="extract_business_from_GCS",
        object_name="yelp_academic_dataset_business.json",
        bucket=BUCKET_NAME,
        filename=f"{BASE_PATH}/data_business.json",
    )

    extract_tips_from_GCS = GCSToLocalFilesystemOperator(
        task_id="extract_tip_from_GCS",
        object_name="yelp_academic_dataset_tip.json",
        bucket=BUCKET_NAME,
        filename=f"{BASE_PATH}/data_tip.json",
    )

    def load_tips(**kwargs):
        '''
        After loading tips data, generate a unique hash id for each of the tips
        '''
        ti = kwargs['ti'] 
        tip_df = pd.read_json(f"{BASE_PATH}/data_tip.json",dtype={'user_id':str,
                             'business_id':str,
                             'date':str,'text':str}, lines=True)
        tip_df = tip_df[['user_id','business_id','text','compliment_count','date']]
        # remove any rows with NA
        tip_df = tip_df.dropna(subset = ['business_id','user_id','text','compliment_count','date'])
        # assign unique id to tips
        tip_df['tips_id'] = tip_df.apply(lambda row : hashlib.sha256(
                            (f"{row['date']}{row['user_id']}{row['business_id']}{len(row['text'])}"
                                ).encode()).hexdigest(), axis = 1)
        tip_df_string = tip_df.to_json(orient = "records")
        ti.xcom_push('tips_data', tip_df_string)
        

    def filter_tips(**kwargs):
        '''
        Filtering out tips linked to food establishments
        '''
        ti = kwargs['ti']
        tips_string = ti.xcom_pull(task_ids='load_tips', key='tips_data')
        tips_data = json.loads(tips_string)
        tips_df = pd.DataFrame(tips_data)

        extract_filtered_business_string = ti.xcom_pull(task_ids='filter_business', key='filtered_business_data')
        filtered_business_data = json.loads(extract_filtered_business_string)
        filtered_business_df = pd.DataFrame(filtered_business_data)

        filtered_tips_df = tips_df[(tips_df.business_id.isin(filtered_business_df.business_id))]
        id_to_names = pd.Series(filtered_business_df.name.values,index=filtered_business_df.business_id).to_dict()
        # map business id to business name
        filtered_tips_df['names'] = filtered_tips_df['business_id'].map(id_to_names)
        tips_df_string = filtered_tips_df.to_json(orient = "records")
        ti.xcom_push('filtered_tips_data', tips_df_string)


    def load_business(**kwargs):
        '''
        Load business data
        '''
        ti = kwargs['ti']         
        business_df = pd.read_json(f"{BASE_PATH}/data_business.json", dtype={'name':str, "city": str, "state" : str,
                        'business_id':str, "review_count" : int, 'stars' : int, "categories" : list, "attributes" : dict,
                            'latitude':str,'longitude':str}, lines=True)
        
        business_df = business_df[['business_id',"name","city","state","stars","review_count","categories","attributes",
                                   "latitude","longitude"]]
        
        # remove any rows with NA (selected colunmns)
        business_df = business_df.dropna(subset = ['business_id','stars','review_count','city','state','categories'])
        business_df_string = business_df.to_json(orient = "records")
        ti.xcom_push('business_data', business_df_string)
              
        
    def filter_business(**kwargs):
        '''
        Filtering out businesses that are food establishments
        '''
        ti = kwargs['ti']   
        extract_business_string = ti.xcom_pull(task_ids='load_business', key='business_data')
        business_data = json.loads(extract_business_string)      
        business_df = pd.DataFrame(business_data)
        business_df['categories'] = business_df['categories'].str.lower()
        food_df  = business_df[(business_df['categories'].str.contains(pat = 'cafe', regex = True)) 
                            | (business_df['categories'].str.contains(pat = 'restaurants', regex = True))
                            | (business_df['categories'].str.contains(pat = 'food', regex = True))
                            ]
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

        # new columns to add
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
        Group businesses by names, aggregate the fields
        '''
        ti = kwargs['ti']
        extract_filtered_business_string = ti.xcom_pull(task_ids='flatten_business', key='flatten_business_data')
        filtered_business_data = json.loads(extract_filtered_business_string)
        filtered_business_df = pd.DataFrame(filtered_business_data)
        name_df = filtered_business_df.groupby(['name']
                    ).agg({'stars' : 'mean', 'review_count' : 'sum', 'name' : 'count',
                        'has_reservation' : lambda x: (x == True).sum(), 'has_delivery' : lambda x: (x == True).sum()}
                    ).rename(columns = {'stars' : 'avg_review','name' : 'total_outlets',
                        'has_reservation' : 'reservation_count', 'has_delivery' : 'delivery_count'}
                    ).reset_index()
        # rename business name to fname
        name_df = name_df.rename(columns = {'name' : 'fname'})
        name_json_string = name_df.to_json(orient="records")
        ti.xcom_push('franchise_data', name_json_string)


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
        cat_df = cat_df[(cat_df.category != 'food') & (cat_df.category != 'restaurants') & (cat_df.category != 'cafes')]
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
            job.result()  # Waits for the job to complete.
            destination_table = bqclient.get_table(f"{PROJECT_ID}.{DATASET_ID}.{BUSINESS_TABLE_ID}")
            print("Loaded {} rows.".format(destination_table.num_rows))
        except Exception as e:
            print(e)


    def load_franchise_to_bq(**kwargs):
        ti = kwargs['ti']
        cat_json_string = ti.xcom_pull(task_ids='group_business', key='franchise_data')
        cat_data = json.loads(cat_json_string)
        try:
            bqclient = bigquery.Client(project = PROJECT_ID)    
            dataset  = bqclient.dataset(DATASET_ID)
            table = dataset.table(FRANCHISE_TABLE_ID)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = bqclient.load_table_from_json(cat_data, table,job_config=job_config)
            job.result()  # Waits for the job to complete.
            destination_table = bqclient.get_table(f"{PROJECT_ID}.{DATASET_ID}.{FRANCHISE_TABLE_ID}")
            print("Loaded {} rows.".format(destination_table.num_rows))
        except Exception as e:
            print("Load job for franchise failed", e)


    def load_tips_to_bq(**kwargs):
        ti = kwargs['ti']
        tips_string = ti.xcom_pull(task_ids='filter_tips', key='filtered_tips_data')
        tips_data = json.loads(tips_string)
        try:
            bqclient = bigquery.Client(project = PROJECT_ID)    
            dataset  = bqclient.dataset(DATASET_ID)
            table = dataset.table(TIPS_TABLE_ID)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = bqclient.load_table_from_json(tips_data, table,job_config=job_config)
            job.result()  # Waits for the job to complete.
            destination_table = bqclient.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TIPS_TABLE_ID}")
            print("Loaded {} rows.".format(destination_table.num_rows))
        except Exception as e:
            print("Load job for tips failed", e)



    def load_category_to_bq(**kwargs):
        ti = kwargs['ti']
        grouped_business_string = ti.xcom_pull(task_ids='count_categories', key='cat_data')
        grouped_business_data = json.loads(grouped_business_string)
        try:
            bqclient = bigquery.Client(project = PROJECT_ID)    
            dataset  = bqclient.dataset(DATASET_ID)
            table = dataset.table(CAT_TABLE_ID)
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = bqclient.load_table_from_json(grouped_business_data, table,job_config=job_config)
            job.result()  # Waits for the job to complete.
            destination_table = bqclient.get_table(f"{PROJECT_ID}.{DATASET_ID}.{CAT_TABLE_ID}")
            print("Loaded {} rows.".format(destination_table.num_rows))

        except Exception as e:
            print("Load job for category failed", e)



    # Python Operators

    load_tips_task = PythonOperator(
        task_id='load_tips',
        python_callable=load_tips,
    )

    filter_tips_task = PythonOperator(
        task_id='filter_tips',
        python_callable=filter_tips,
    )
    
    load_business_task = PythonOperator(
        task_id='load_business',
        python_callable=load_business,
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

    load_franchise_bq = PythonOperator(
        task_id='load_franchise_to_bq',
        python_callable=load_franchise_to_bq,
    )

    load_tips_bq = PythonOperator(
        task_id='load_tips_to_bq',
        python_callable=load_tips_to_bq,
    )

    load_categories_bq = PythonOperator(
        task_id='load_cat_to_bq',
        python_callable=load_category_to_bq,
    )


extract_business_from_GCS >> load_business_task >> filtered_business_task >> flatten_business_task >> load_business_bq
flatten_business_task >> group_business_task >> load_franchise_bq
filtered_business_task >> count_categories_task >> load_categories_bq
extract_tips_from_GCS >> load_tips_task >> filter_tips_task
filtered_business_task >> filter_tips_task >> load_tips_bq
