# yelp_airflow
Data engineering project

![image](https://user-images.githubusercontent.com/77261306/233347338-d2f28848-9cb4-42f0-b571-3805702931c1.png)

## 1. Project Description
The F&B Industry in the US is heavily dominated by big-name franchises which take up a large market share, making it difficult for smaller cafes, restaurants and eateries to break into the higher echelons of the food industry. We believe that one major hindrance in bridging this gap is the lack of resources and information of what exactly is required of a high-quality food store and what it takes to achieve the threshold benchmark. As such, business owners are hesitant in investing resources 
to push their businesses up to the higher-quality food markets. 

#### Objectives/Goals
Our project aims to:
1. Identify reasons for the disparity between highly rated and lower rated food stores in a specific city.
2. Provide guidance for potential food startups on factors such as location and food categories needed to achieve a higher rating.
3. Identify areas for improvement and rectification in existing businesses.
4. Determine what consumers value in a food business.

By building a robust and efficient ETL pipeline that can process raw batch data and store them in query-able forms, we hope to facilitate the generation of valuable business insights for our stakeholders. Downstream applications like Tableau will be used to exhibit the benefits and use cases of the pipeline.

#### [Source Data](https://www.yelp.com/dataset)

## 2. Setup
For this project, we are running on Python 3.8.10 and Airflow 2.5.2. Python libraries like pandas, numpy, nltk, matplotlib will be used.We are also using [google’s provider package](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/index.html) to utilize the google cloud services. A shared service account key is used to authenticate with the google cloud services.

### A. Create project

In your google cloud platform console, create a new project.

![image](https://user-images.githubusercontent.com/77261306/233475928-74dbfa2d-6996-4cb0-9540-f1e1392ee3e7.png)


### B. Create service account
Under IAM and Admin, set up your Google Cloud Platform service account by granting the right admin and access privileges. Granted roles include Editor and Storage Admin. Once created, you can view the service account on the main page. It is important that your service account is created under the project you have just initialised.

Click into the service account and navigate to the Keys tab to create a private key. Select JSON as the key type and upon creation, the key will be automatically downloaded into your computer. Keep track of the path to where the key is stored for section E.

### C. Create Google Cloud Storage and Buckets
Under Cloud Storage, go to buckets and click create. Create a storage bucket with a unique name, and the rest of the settings can remain at the default selected ones. Once the bucket has been created, select the bucket and upload files. Upload the 2 JSON files from the yelp dataset for business and tips. The final product should look something like this:
![image](https://user-images.githubusercontent.com/77261306/233565525-846c330a-792e-4e8e-9c62-1156807cef84.png)

### D. Initialise Big Query
Under the BigQuery, create a dataset under this project.
* Data set ID (example: yelp_dataset)
* Data location. Choose the nearest one from your current location.

### E. Set up variables in Airflow
![image](https://user-images.githubusercontent.com/77261306/233475751-5cb5c87b-2a34-434d-bcca-dd1ad4d5816a.png)

Under the yelp_dag.py file, fill in the variables accordingly. Alternatively, the variables can be stored as global variables on Airflow webserver > admin > variables.

* Credentials is the file path to where your service account json file is stored.
* Base path is where you want the data to be stored locally after extracting them from GCS
* Bucket name is the name of your bucket created in Section C.
* Change the project id to your own project id based on your [project](console.cloud.google.com)
* Under BigQuery, copy and change the dataset id. (For example, for a copied dataset id “yelp-test-384317.yelp_dataset”, “yelp_dataset” will be the dataset id)
* Table ids will be the ID of your table once the DAG is executed. Ensure it follows the [naming convention](https://cloud.google.com/bigquery/docs/tables#:~:text=When%20you%20create%20a%20table,)
* To establish connection to Google cloud, you must set:
```
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS
```

### F. Running airflow
To run airflow webserver and scheduler, run the following commands
```
airflow webserver –port [port number]
airflow scheduler
````
* Execute the yelp_dag file 

