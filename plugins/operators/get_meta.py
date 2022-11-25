from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pandas as pd
import boto3
from io import StringIO

class GetMetaOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 variable = "",
                 key = "",
                 url_lang = "",
                 data_name = "",
                 *args, **kwargs
                 ):
        
        super(GetMetaOperator, self).__init__(*args, **kwargs)
        self.variable = variable
        self.key = key
        self.url_lang = url_lang
        self.data_name = data_name
        
    def execute(self, context):

        self.log.info("Pull data from GUS API and area variable metadata into S3 bucket")
        url = f"https://api-dbw.stat.gov.pl/api/1.1.0/variable/variable-meta?id-zmiennej={self.variable}&lang={self.url_lang}"
        response = requests.get(url)
        data = response.json()
        df_data = pd.DataFrame(data[self.key])
        
        df_data.columns = df_data.columns.str.replace('-','_')
        for column_name in df_data.columns:
            if df_data[column_name].dtype in ['str','O']:
                try:
                    df_data[column_name] = df_data[column_name].str.replace('<P>','').str.replace('</P>','').str.replace('<p>\n\t','').str.replace('<p>','').\
                                str.replace('</p>','').str.replace('\n','').str.replace('<sup>','').str.replace('</sup>','').str.replace('<SUP>','').str.replace('</SUP>','')
                except:
                    continue
                
        csv_buffer = StringIO()
        df_data.to_csv(csv_buffer)
        bucket = 'gus-dbw-rawdata'
        hook = S3Hook(aws_conn_id="s3_conn")
        s3_object_name = f"{self.data_name}.csv"
        s3_object_path = f"metadata/{self.variable}/{s3_object_name}"
        hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=s3_object_path,
            bucket_name=bucket,
            replace=True
                )