from datetime import datetime, timedelta
import time
import os
import boto3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.create_tables import CreateTablesOperator
from operators.load_redshift import LoadToRedshiftOperator
from operators.load_table import LoadTableOperator
from airflow.operators.python import PythonOperator
from helpers import SqlQueries

default_args = {
    'owner': 'pwlcpl',
    'start_date': datetime(2022, 9, 1),
    'end_date' : datetime.utcnow(),
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    #'email': ['xxx@xxx']
}

#schedule - each month - 10th
dag_db = DAG('gus_s3_to_db',
          default_args = default_args,
          description = 'Gets data from S3 and loads it into Aws Redshift',
          schedule_interval = '0 0 10 * *'
            )

create_tables_in_redshift = CreateTablesOperator(
    task_id="Create_tables",
    dag=dag_db,
    redshift_conn_id='redshift',
    sql=SqlQueries.dwh_tables_create
    )
##################################

load_area_dictionary = LoadToRedshiftOperator(
    task_id='Load_areas',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    #aws_credentials_id = 's3_conn',
    table="staging_dict_areas",
    #csv_columns = "(id, nazwa, id_nadrzedny_element, id_poziom, nazwa_poziom, czy_zmienne)",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    #execution_date = kwargs["execution_date"],
    #execution_date = datetime.utcnow(),
    s3_key=["dictionaries/dict_areas.csv"]
    #json_path="s3://udacity-dend/log_json_path.json",
    #provide_context=True    
    )

load_area_variables_dictionary = LoadToRedshiftOperator(
    task_id='Load_wage_variables_dictionaries',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_area_variables",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_wages_variables.csv","dictionaries/dict_real_estate_variables.csv","dictionaries/dict_retail_prices_variables.csv"]
    )

load_variables_dictionary = LoadToRedshiftOperator(
    task_id='Load_variables_dictionary',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_variables",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_variables.csv"]
    )

load_dims_positions_dictionary = LoadToRedshiftOperator(
    task_id='Load_dims_positions_dictionaries',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_dims_sections",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_dims_sections_307.csv","dictionaries/dict_dims_sections_400.csv","dictionaries/dict_dims_sections_303.csv"]
    )

load_pres_methodology_dictionary = LoadToRedshiftOperator(
    task_id='Load_presentation_methodology_dictionary',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_pres_methods",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_pres_methods.csv"]
    )

load_periods_dictionary = LoadToRedshiftOperator(
    task_id='Load_periods_dictionary',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_periods",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_periods.csv"]
    )

load_dates_dictionary = LoadToRedshiftOperator(
    task_id='Load_dates_dictionary',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_dates",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_dates.csv"]
    )

load_novalue_reason_dictionary = LoadToRedshiftOperator(
    task_id='Load_novalue_reason_dictionary',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_novalue_reason",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_novalue_reason.csv"]
    )

load_confidentiality_code_dictionary = LoadToRedshiftOperator(
    task_id='Load_confidentiality_code_dictionary',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_confidentiality_code",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_confidentiality_code.csv"]
    )

load_flags_dictionary = LoadToRedshiftOperator(
    task_id='Load_flags_dictionary',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_flags",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_flags.csv"]
    )

load_sections_metadata = LoadToRedshiftOperator(
    task_id='Load_sections_metadata',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sections",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_sections.csv","metadata/400/400_sections.csv","metadata/303/303_sections.csv"]
    )

load_terms_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_terms_metadata_307',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_terms_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_terms.csv"]
    )

load_terms_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_terms_metadata_400',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_terms_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_terms.csv"]
    )

load_terms_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_terms_metadata_303',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_terms_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_terms.csv"]
    )

load_departments_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_departments_metadata_307',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_departments_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_departments.csv"]
    )

load_departments_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_departments_metadata_400',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_departments_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_departments.csv"]
    )

load_departments_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_departments_metadata_303',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_departments_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_departments.csv"]
    )

load_methodology_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_methodology_metadata_307',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_methodology_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_methodology.csv"]
    )

load_methodology_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_methodology_metadata_400',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_methodology_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_methodology.csv"]
    )

load_methodology_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_methodology_metadata_303',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_methodology_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_methodology.csv"]
    )

load_sets_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_sets_metadata_307',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sets_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_sets.csv"]
    )

load_sets_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_sets_metadata_400',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sets_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_sets.csv"]
    )

load_sets_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_sets_metadata_303',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sets_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_sets.csv"]
    )

load_studies_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_studies_metadata_307',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_studies_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_studies.csv"]
    )

load_studies_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_studies_metadata_400',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_studies_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_studies.csv"]
    )

load_studies_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_studies_metadata_303',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_studies_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_studies.csv"]
    )

load_tags_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_tags_metadata_307',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_tags_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_tags.csv"]
    )

load_tags_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_tags_metadata_400',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_tags_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_tags.csv"]
    )

load_tags_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_tags_metadata_303',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_tags_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_tags.csv"]
    )

load_real_estate_logs = LoadToRedshiftOperator(
    task_id='Load_real_estate_logs',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_realestate_prices",
    truncate = False,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["facts/307/"]
    )

load_wages_logs = LoadToRedshiftOperator(
    task_id='Load_wages_logs',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_wages",
    truncate = False,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["facts/400/"]
    )

load_retail_prices_logs = LoadToRedshiftOperator(
    task_id='Load_retail_prices_logs',
    dag=dag_db,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_retail_prices",
    truncate = False,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["facts/303/"]
    )

insert_into_areas_table = LoadTableOperator(
    task_id='Insert_data_into_areas_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="areas",
    sql=SqlQueries.areas_table_insert,
    truncate = True
    )

insert_into_variables_table = LoadTableOperator(
    task_id='Insert_data_into_variables_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="variables",
    sql=SqlQueries.variables_table_insert,
    truncate = True
    )

insert_into_sections_periods_table = LoadTableOperator(
    task_id='Insert_data_into_sections_periods_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="sections_periods",
    sql=SqlQueries.sections_periods_table_insert,
    truncate = True
    )

insert_into_dims_positions_table = LoadTableOperator(
    task_id='Insert_data_into_dims_positions_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="dims_positions",
    sql=SqlQueries.dims_positions_table_insert,
    truncate = True
    )

insert_into_pres_method_table = LoadTableOperator(
    task_id='Insert_data_into_pres_method_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="pres_method",
    sql=SqlQueries.pres_method_table_insert,
    truncate = True
    )

insert_into_periods_table = LoadTableOperator(
    task_id='Insert_data_into_periods_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="periods",
    sql=SqlQueries.periods_table_insert,
    truncate = True
    )

insert_into_dates_table = LoadTableOperator(
    task_id='Insert_data_into_dates_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="dates",
    sql=SqlQueries.dates_table_insert,
    truncate = True
    )

insert_into_novalue_reason_table = LoadTableOperator(
    task_id='Insert_data_into_novalue_reason_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="novalue_reason",
    sql=SqlQueries.novalue_reason_table_insert,
    truncate = True
    )

insert_into_confidentiality_code_table = LoadTableOperator(
    task_id='Insert_data_into_confidentiality_code_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="confidentiality_code",
    sql=SqlQueries.confidentiality_code_table_insert,
    truncate = True
    )

insert_into_flags_table = LoadTableOperator(
    task_id='Insert_data_into_flags_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="flags",
    sql=SqlQueries.flags_table_insert,
    truncate = True
    )

insert_into_sections_table = LoadTableOperator(
    task_id='Insert_data_into_sections_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="sections",
    sql=SqlQueries.sections_table_insert,
    truncate = True
    )

insert_307_into_terms_table = LoadTableOperator(
    task_id='Insert_307_data_into_terms_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="terms",
    sql=SqlQueries.terms307_table_insert,
    truncate = True
    )

insert_400_into_terms_table = LoadTableOperator(
    task_id='Insert_400_data_into_terms_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="terms",
    sql=SqlQueries.terms400_table_insert,
    truncate = False
    )

insert_303_into_terms_table = LoadTableOperator(
    task_id='Insert_303_data_into_terms_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="terms",
    sql=SqlQueries.terms303_table_insert,
    truncate = False
    )

insert_307_into_departments_table = LoadTableOperator(
    task_id='Insert_307_data_into_departments_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="departments",
    sql=SqlQueries.departments307_table_insert,
    truncate = True
    )

insert_400_into_departments_table = LoadTableOperator(
    task_id='Insert_400_data_into_departments_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="departments",
    sql=SqlQueries.departments400_table_insert,
    truncate = False
    )

insert_303_into_departments_table = LoadTableOperator(
    task_id='Insert_303_data_into_departments_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="departments",
    sql=SqlQueries.departments303_table_insert,
    truncate = False
    )

insert_307_into_methodology_table = LoadTableOperator(
    task_id='Insert_307_data_into_methodology_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="methodology",
    sql=SqlQueries.methodology307_table_insert,
    truncate = True
    )

insert_400_into_methodology_table = LoadTableOperator(
    task_id='Insert_400_data_into_methodology_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="methodology",
    sql=SqlQueries.methodology400_table_insert,
    truncate = False
    )

insert_303_into_methodology_table = LoadTableOperator(
    task_id='Insert_303_data_into_methodology_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="methodology",
    sql=SqlQueries.methodology303_table_insert,
    truncate = False
    )

insert_307_into_sets_table = LoadTableOperator(
    task_id='Insert_307_data_into_sets_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="sets",
    sql=SqlQueries.sets307_table_insert,
    truncate = True
    )

insert_400_into_sets_table = LoadTableOperator(
    task_id='Insert_400_data_into_sets_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="sets",
    sql=SqlQueries.sets400_table_insert,
    truncate = False
    )

insert_303_into_sets_table = LoadTableOperator(
    task_id='Insert_303_data_into_sets_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="sets",
    sql=SqlQueries.sets303_table_insert,
    truncate = False
    )

insert_307_into_studies_table = LoadTableOperator(
    task_id='Insert_307_data_into_studies_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="studies",
    sql=SqlQueries.studies307_table_insert,
    truncate = True
    )

insert_400_into_studies_table = LoadTableOperator(
    task_id='Insert_400_data_into_studies_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="studies",
    sql=SqlQueries.studies400_table_insert,
    truncate = False
    )

insert_303_into_studies_table = LoadTableOperator(
    task_id='Insert_303_data_into_studies_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="studies",
    sql=SqlQueries.studies303_table_insert,
    truncate = False
    )

insert_307_into_tags_table = LoadTableOperator(
    task_id='Insert_307_data_into_tags_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="tags",
    sql=SqlQueries.tags307_table_insert,
    truncate = True
    )

insert_400_into_tags_table = LoadTableOperator(
    task_id='Insert_400_data_into_tags_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="tags",
    sql=SqlQueries.tags400_table_insert,
    truncate = False
    )

insert_303_into_tags_table = LoadTableOperator(
    task_id='Insert_303_data_into_tags_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="tags",
    sql=SqlQueries.tags303_table_insert,
    truncate = False
    )

insert_wages_into_logs_table = LoadTableOperator(
    task_id='Insert_wages_data_into_logs_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="logs",
    sql=SqlQueries.logs_wages_table_insert,
    truncate = False
    )

insert_retail_prices_into_logs_table = LoadTableOperator(
    task_id='Insert_retail_prices_data_into_logs_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="logs",
    sql=SqlQueries.logs_retail_prices_table_insert,
    truncate = False
    )

insert_realestate_into_logs_table = LoadTableOperator(
    task_id='Insert_realestate_data_into_logs_table',
    dag=dag_db,
    redshift_conn_id='redshift',
    table="logs",
    sql=SqlQueries.logs_realestate_table_insert,
    truncate = False
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag_db)
end_operator = DummyOperator(task_id="Stop_execution", dag=dag_db)

start_operator >> create_tables_in_redshift
create_tables_in_redshift >> load_area_dictionary
create_tables_in_redshift >> load_area_variables_dictionary
create_tables_in_redshift >> load_variables_dictionary
create_tables_in_redshift >> load_dims_positions_dictionary
create_tables_in_redshift >> load_pres_methodology_dictionary
create_tables_in_redshift >> load_periods_dictionary
create_tables_in_redshift >> load_dates_dictionary
create_tables_in_redshift >> load_novalue_reason_dictionary
create_tables_in_redshift >> load_confidentiality_code_dictionary
create_tables_in_redshift >> load_flags_dictionary
create_tables_in_redshift >> load_sections_metadata
create_tables_in_redshift >> load_terms_metadata_307
create_tables_in_redshift >> load_terms_metadata_400
create_tables_in_redshift >> load_terms_metadata_303
create_tables_in_redshift >> load_departments_metadata_307
create_tables_in_redshift >> load_departments_metadata_400
create_tables_in_redshift >> load_departments_metadata_303
create_tables_in_redshift >> load_methodology_metadata_307
create_tables_in_redshift >> load_methodology_metadata_400
create_tables_in_redshift >> load_methodology_metadata_303
create_tables_in_redshift >> load_sets_metadata_307
create_tables_in_redshift >> load_sets_metadata_400
create_tables_in_redshift >> load_sets_metadata_303
create_tables_in_redshift >> load_studies_metadata_307
create_tables_in_redshift >> load_studies_metadata_400
create_tables_in_redshift >> load_studies_metadata_303
create_tables_in_redshift >> load_tags_metadata_307
create_tables_in_redshift >> load_tags_metadata_400
create_tables_in_redshift >> load_tags_metadata_303
create_tables_in_redshift >> load_real_estate_logs
create_tables_in_redshift >> load_wages_logs
create_tables_in_redshift >> load_retail_prices_logs

load_area_dictionary >> insert_into_areas_table
load_area_variables_dictionary >> insert_into_variables_table
load_variables_dictionary >> insert_into_sections_periods_table
load_dims_positions_dictionary >> insert_into_dims_positions_table
load_pres_methodology_dictionary >> insert_into_pres_method_table
load_periods_dictionary >> insert_into_periods_table
load_dates_dictionary >> insert_into_dates_table
load_novalue_reason_dictionary >> insert_into_novalue_reason_table
load_confidentiality_code_dictionary >> insert_into_confidentiality_code_table
load_flags_dictionary >> insert_into_flags_table
load_sections_metadata >> insert_into_sections_table
load_terms_metadata_307 >> insert_307_into_terms_table
load_terms_metadata_400 >> insert_400_into_terms_table
load_terms_metadata_303 >> insert_303_into_terms_table
insert_307_into_terms_table >> insert_400_into_terms_table
insert_400_into_terms_table >> insert_303_into_terms_table
load_departments_metadata_307 >> insert_307_into_departments_table
load_departments_metadata_400 >> insert_400_into_departments_table
load_departments_metadata_303 >> insert_303_into_departments_table
insert_307_into_departments_table >> insert_400_into_departments_table
insert_400_into_departments_table >> insert_303_into_departments_table
load_methodology_metadata_307 >> insert_307_into_methodology_table
load_methodology_metadata_400 >> insert_400_into_methodology_table
load_methodology_metadata_303 >> insert_303_into_methodology_table
insert_307_into_methodology_table >> insert_400_into_methodology_table
insert_400_into_methodology_table >> insert_303_into_methodology_table
load_sets_metadata_307 >> insert_307_into_sets_table
load_sets_metadata_400 >> insert_400_into_sets_table
load_sets_metadata_303 >> insert_303_into_sets_table
insert_307_into_sets_table >> insert_400_into_sets_table
insert_400_into_sets_table >> insert_303_into_sets_table
load_studies_metadata_307 >> insert_307_into_studies_table
load_studies_metadata_400 >> insert_400_into_studies_table
load_studies_metadata_303 >> insert_303_into_studies_table
insert_307_into_studies_table >> insert_400_into_studies_table
insert_400_into_studies_table >> insert_303_into_studies_table
load_tags_metadata_307 >> insert_307_into_tags_table
load_tags_metadata_400 >> insert_400_into_tags_table
load_tags_metadata_303 >> insert_303_into_tags_table
insert_307_into_tags_table >> insert_400_into_tags_table
insert_400_into_tags_table >> insert_303_into_tags_table
load_real_estate_logs >> insert_wages_into_logs_table
load_wages_logs >> insert_retail_prices_into_logs_table
load_retail_prices_logs >> insert_realestate_into_logs_table
insert_wages_into_logs_table >> insert_retail_prices_into_logs_table
insert_retail_prices_into_logs_table >> insert_realestate_into_logs_table

insert_into_areas_table >> end_operator
insert_into_variables_table >> end_operator
insert_into_sections_periods_table >> end_operator
insert_into_dims_positions_table >> end_operator
insert_into_pres_method_table >> end_operator
insert_into_periods_table >> end_operator
insert_into_dates_table >> end_operator
insert_into_novalue_reason_table >> end_operator
insert_into_confidentiality_code_table >> end_operator
insert_into_flags_table >> end_operator
insert_into_sections_table >> end_operator
insert_303_into_terms_table >> end_operator
insert_303_into_departments_table >> end_operator
insert_303_into_methodology_table >> end_operator
insert_303_into_sets_table >> end_operator
insert_303_into_studies_table >> end_operator
insert_303_into_tags_table >> end_operator
insert_realestate_into_logs_table >> end_operator
