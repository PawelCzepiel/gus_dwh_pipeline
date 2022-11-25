from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs
                  ):
        
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        
    def execute(self, context):
        self.log.info("Creating tables if don't exist") 
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        redshift_hook.run(self.sql)  