from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks="",
                 tables = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Data Quality Check for null values")
        error_count = 0
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            
            records_query = redshift.get_records(sql)[0]
            if exp_result != records_query[0]:
                error_count += 1
                failing_tests.append(sql)
                
        if error_count > 0:
            self.log.info('Null Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info('Null Tests passed')            
            
        for table in self.tables:
            self.log.info(f"Data Quality Check - {table} table")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed - {table} table returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed - {table} table contained 0 rows")
            logging.info(f"Data quality check on {table} table passed with {records[0][0]} records")