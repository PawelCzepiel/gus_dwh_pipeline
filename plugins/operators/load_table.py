from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate="",
                 *args, **kwargs):

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        self.log.info(f"Loading {self.table} table") 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        if self.truncate:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE {}".format(self.table))
           
        self.log.info(f"Loading {self.table} dimension table")               
        redshift.run(self.sql)