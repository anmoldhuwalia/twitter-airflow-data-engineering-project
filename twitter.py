from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import tweepy
from tweepy import OAuthHandler
import pandas as pd 
import json
from datetime import datetime
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO


def run_twitter_etl():

    access_key = "aMmjiALf9Cc4zMODThsjTLLBg" 
    access_secret = "EqFQXUTWOl6FSnl9rp9Fi4Q4JAYqjqrrF7gjHeydvNMEM14Yvl" 
    consumer_key = "1396482917715484673-GQIJUmYizUnFaNwA3EC0pA5zZpfqsW"
    consumer_secret = "wmI2G5VZdQKXM9YgY84TznXaPIPPeQvIykcAXeFH089Tm"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@narendramodi', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)

    # Save DataFrame to CSV in-memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Upload CSV to Azure Blob Storage
    azure_storage_connection_string = os.getenv('DefaultEndpointsProtocol=https;AccountName=twitterdataextraction;AccountKey=38wZ99/gjWv3sd2WV+H/wt3QWmtkxqUjmOLx0OVDMEPSdjeHrpfGS8TB7PPL989E/d3N/hdKL3wC+AStyVO7oQ==;EndpointSuffix=core.windows.net')
    container_name = 'twitterdata'  # Azure Blob container name
    blob_name = 'twitter_data/twitter_data.csv'  # Desired Blob name
    
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
    
    print(f"Twitter data saved to Azure Blob Storage at {container_name}/{blob_name}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable= run_twitter_etl,
    dag=dag, 
)

run_etl






