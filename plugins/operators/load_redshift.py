from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadToRedshiftOperator(BaseOperator):

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        CSV
        REGION 'eu-central-1'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 truncate = "",
                 s3_bucket="",
                 s3_key="", 
                 *args, **kwargs):

        super(LoadToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.truncate = truncate
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
                    
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE {}".format(self.table))
            
        self.log.info("Copying data from S3 to Redshift")
        for file_name in self.s3_key:
            s3_path = "s3://{}/{}".format(self.s3_bucket, file_name)
            formatted_sql = LoadToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
                )
            redshift.run(formatted_sql)