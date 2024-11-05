import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from os.path import join
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
        
        twitter_operator = TwitterOperator(
            file_path=join("datalake/bronze/twitter_datascience",
                           "extract_date={{ ds }}",
                           "datascience_{{ ds_nodash }}.json"),                       
            query=query, 
            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
            task_id="twitter_datascience"
            )
        
        twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
                                                application="/home/lucaires/Documentos/airflow_twitter/src/spark/transformation.py",
                                                name="twitter_transformation",
                                                application_args=["--src", "datalake/bronze/twitter_datascience", 
                                                                  "--dest", "datalake/silver/twitter_datascience",
                                                                  "--process-date", "{{ ds }}"]
                                                )
        
        twitter_operator >> twitter_transform