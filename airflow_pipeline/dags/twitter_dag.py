import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from airflow.utils.dates import days_ago

with DAG(
        dag_id="TwitterDag",
        start_date=days_ago(2), 
        schedule="@daily"
    ) as dag:
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
        query = "data science"
        
        to = TwitterOperator(
            file_path=os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..','datalake', 'twitter_datascience')),
                           "extract_date={{ ds }}",
                           "datascience_{{ ds_nodash }}.json"),                       
            query=query, 
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
            task_id="twitter_datascience"
            )
        