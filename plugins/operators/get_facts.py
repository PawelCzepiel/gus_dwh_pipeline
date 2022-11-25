from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pandas as pd
import boto3
from io import StringIO
import time

class GetFactsOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 url_param = "",
                 variable = "",
                 section = "",
                 year_start = "",
                 year_end = "",
                 period_start = "",
                 period_end = "",
                 page0 = "",
                 url_lang = "",
                 *args, **kwargs
                 ):
        
        super(GetFactsOperator, self).__init__(*args, **kwargs)
        self.url_param = url_param
        self.variable = variable
        self.section = section
        self.year_start = year_start
        self.year_end = year_end
        self.period_start = period_start
        self.period_end = period_end
        self.page0 = page0
        self.url_lang = url_lang

    def execute(self, context):

        self.log.info("Pull data from GUS API and load variable facts into S3 bucket")
        url_base = "https://api-dbw.stat.gov.pl/api/1.1.0/"
        for year in range(self.year_start, self.year_end+1):
            for period in range(self.period_start, self.period_end+1):
                time.sleep(3)
                url = f"{url_base}{self.url_param}id-zmienna={self.variable}\
                        &id-przekroj={self.section}&id-rok={year}&id-okres={period}\
                        &ile-na-stronie=5000&numer-strony={self.page0}&lang={self.url_lang}"
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    df_data = pd.DataFrame(data['data'])
                    page_count = data['page-count']
                    if page_count > 0:
                        for page in range(page_count+1):
                            url = f"{url_base}{self.url_param}id-zmienna={self.variable}\
                                    &id-przekroj={self.section}&id-rok={year}&id-okres={period}\
                                    &ile-na-stronie=5000&numer-strony={page}&lang={self.url_lang}"
                            response = requests.get(url)
                            data = response.json()
                            df_data = pd.concat([df_data,pd.DataFrame(data['data'])],axis=0)
                    df_data.columns = df_data.columns.str.replace('-','_')
                    for column_name in df_data.columns:
                        if df_data[column_name].dtype in ['str','O']:
                            try:
                                df_data[column_name] = df_data[column_name].str.replace('<P>','').str.replace('</P>','').\
                                    str.replace('<p>\n\t','').str.replace('<p>','').\
                                    str.replace('</p>','').str.replace('\n','').str.replace('<sup>','').\
                                    str.replace('</sup>','').str.replace('<SUP>','').str.replace('</SUP>','')
                            except:
                                continue
                    csv_buffer = StringIO()
                    df_data.to_csv(csv_buffer)
                    bucket = "gus-dbw-rawdata"
                    hook = S3Hook(aws_conn_id="s3_conn")
                    s3_object_path = f"facts/{self.variable}/{self.variable}_{self.section}_{year}_{period}.csv"
                    hook.load_string(
                        string_data=csv_buffer.getvalue(),
                        key=s3_object_path,
                        bucket_name=bucket,
                        replace=True
                            )
                    df_data.drop(df_data.index, inplace=True)
                else:
                    continue