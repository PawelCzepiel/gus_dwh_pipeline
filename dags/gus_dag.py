from datetime import datetime, timedelta
import time
import os
import boto3
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.get_data import GetDataOperator
from operators.get_meta import GetMetaOperator
from operators.get_facts import GetFactsOperator
from airflow.operators.python import PythonOperator
from operators.create_tables import CreateTablesOperator
from operators.load_redshift import LoadToRedshiftOperator
from operators.load_table import LoadTableOperator
from operators.data_quality import DataQualityOperator
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
dag = DAG('gus',
          default_args = default_args,
          description = 'Gets data from Gus dbw api and loads it into Aws Redshift',
          schedule_interval = '0 0 10 * *'
            )

get_areas = GetDataOperator(
        task_id = "Get_areas",
        dag = dag,
        url_param = "area/area-area?",
        data_name = "dict_areas",
        page0 = '',
        url_lang = 'en'
                )

get_variables = GetDataOperator(
        task_id = "Get_variables",
        dag = dag,
        url_param = "variable/variable-section-periods?",
        data_name = "dict_variables",
        page0 = 0,
        url_lang = 'pl'
                )

get_dates = GetDataOperator(
        task_id = "Get_dates",
        dag = dag,
        url_param = "dictionaries/date-dictionary?",
        data_name = "dict_dates",
        page0 = 0,
        url_lang = 'pl'
                )

get_periods = GetDataOperator(
        task_id = "Get_periods",
        dag = dag,
        url_param = "dictionaries/periods-dictionary?",
        data_name = "dict_periods",
        page0 = 0,
        url_lang = 'en'
                )

get_pres_method = GetDataOperator(
        task_id = "Get_pres_method",
        dag = dag,
        url_param = "dictionaries/way-of-presentation?",
        data_name = "dict_pres_methods",
        page0 = 0,
        url_lang = 'en'
                )

get_novalue_reason = GetDataOperator(
        task_id = "Get_novalue_reason",
        dag = dag,
        url_param = "dictionaries/no-value-dictionary?",
        data_name = "dict_novalue_reason",
        page0 = 0,
        url_lang = 'en'
                )

get_confidentiality_code = GetDataOperator(
        task_id = "Get_confidentiality_code",
        dag = dag,
        url_param = "dictionaries/confidentionality-dictionary?",
        data_name = "dict_confidentiality_code",
        page0 = 0,
        url_lang = 'en'
                )

get_flags = GetDataOperator(
        task_id = "Get_flags",
        dag = dag,
        url_param = "dictionaries/flag-dictionary?",
        data_name = "dict_flags",
        page0 = 0,
        url_lang = 'en'
                )

get_real_estate_variables = GetDataOperator(
        task_id = "Get_real_estate_variables",
        dag = dag,
        url_param = "area/area-variable?id-obszaru=12",
        data_name = "dict_real_estate_variables",
        page0 = '',
        url_lang = 'en'
                )

get_wages_variables = GetDataOperator(
        task_id = "Get_wages_variables",
        dag = dag,
        url_param = "area/area-variable?id-obszaru=227",
        data_name = "dict_wages_variables",
        page0 = '',
        url_lang = 'en'
                )

get_retail_prices_variables = GetDataOperator(
        task_id = "Get_retail_prices_variables",
        dag = dag,
        url_param = "area/area-variable?id-obszaru=8",
        data_name = "dict_retail_prices_variables",
        page0 = '',
        url_lang = 'en'
                )

get_307_dims_sections = GetDataOperator(
        task_id = "Get_307_dims_and_sections",
        dag = dag,
        url_param = "variable/variable-section-position?id-przekroj=485",
        data_name = "dict_dims_sections_307",
        page0 = '',
        url_lang = 'en'
                )

get_400_dims_sections = GetDataOperator(
        task_id = "Get_400_dims_and_sections",
        dag = dag,
        url_param = "variable/variable-section-position?id-przekroj=2",
        data_name = "dict_dims_sections_400",
        page0 = '',
        url_lang = 'en'
                )

get_303_dims_sections = GetDataOperator(
        task_id = "Get_303_dims_and_sections",
        dag = dag,
        url_param = "variable/variable-section-position?id-przekroj=740",
        data_name = "dict_dims_sections_303",
        page0 = '',
        url_lang = 'en'
                )

get_307_tags = GetMetaOperator(
        task_id = "Get_tags_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "tagi",
        data_name = "307_tags",
        url_lang = 'en'
                )

get_307_terms = GetMetaOperator(
        task_id = "Get_terms_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "pojecia",
        data_name = "307_terms",
        url_lang = 'en'
                )

get_307_method = GetMetaOperator(
        task_id = "Get_methodology_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "wyjasnienia-metodologiczne",
        data_name = "307_methodology",
        url_lang = 'en'
                )

get_307_sections = GetMetaOperator(
        task_id = "Get_sections_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "przekroje",
        data_name = "307_sections",
        url_lang = 'en'
                )

get_307_sets = GetMetaOperator(
        task_id = "Get_sets_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "zestawy",
        data_name = "307_sets",
        url_lang = 'en'
                )

get_307_studies = GetMetaOperator(
        task_id = "Get_studies_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "badania",
        data_name = "307_studies",
        url_lang = 'en'
                )

get_307_acts = GetMetaOperator(
        task_id = "Get_legal_acts_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "akty-prawne",
        data_name = "307_acts",
        url_lang = 'en'
                )

get_307_departments = GetMetaOperator(
        task_id = "Get_gov_departments_for_m2_price_median",
        dag = dag,
        variable = 307,
        key = "jednostki",
        data_name = "307_departments",
        url_lang = 'en'
                )

get_400_tags = GetMetaOperator(
        task_id = "Get_tags_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "tagi",
        data_name = "400_tags",
        url_lang = 'en'
                )

get_400_terms = GetMetaOperator(
        task_id = "Get_terms_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "pojecia",
        data_name = "400_terms",
        url_lang = 'en'
                )

get_400_method = GetMetaOperator(
        task_id = "Get_methodology_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "wyjasnienia-metodologiczne",
        data_name = "400_methodology",
        url_lang = 'en'
                )

get_400_sections = GetMetaOperator(
        task_id = "Get_sections_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "przekroje",
        data_name = "400_sections",
        url_lang = 'en'
                )

get_400_sets = GetMetaOperator(
        task_id = "Get_sets_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "zestawy",
        data_name = "400_sets",
        url_lang = 'en'
                )

get_400_studies = GetMetaOperator(
        task_id = "Get_studies_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "badania",
        data_name = "400_studies",
        url_lang = 'en'
                )

get_400_acts = GetMetaOperator(
        task_id = "Get_legal_acts_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "akty-prawne",
        data_name = "400_acts",
        url_lang = 'en'
                )

get_400_departments = GetMetaOperator(
        task_id = "Get_gov_departments_for_avg_monthly_wages",
        dag = dag,
        variable = 400,
        key = "jednostki",
        data_name = "400_departments",
        url_lang = 'en'
                )

get_303_tags = GetMetaOperator(
        task_id = "Get_tags_for_food_prices",
        dag = dag,
        variable = 303,
        key = "tagi",
        data_name = "303_tags",
        url_lang = 'en'
                )

get_303_terms = GetMetaOperator(
        task_id = "Get_terms_for_food_prices",
        dag = dag,
        variable = 303,
        key = "pojecia",
        data_name = "303_terms",
        url_lang = 'en'
                )

get_303_method = GetMetaOperator(
        task_id = "Get_methodology_for_food_prices",
        dag = dag,
        variable = 303,
        key = "wyjasnienia-metodologiczne",
        data_name = "303_methodology",
        url_lang = 'en'
                )

get_303_sections = GetMetaOperator(
        task_id = "Get_sections_for_food_prices",
        dag = dag,
        variable = 303,
        key = "przekroje",
        data_name = "303_sections",
        url_lang = 'en'
                )

get_303_sets = GetMetaOperator(
        task_id = "Get_sets_for_food_prices",
        dag = dag,
        variable = 303,
        key = "zestawy",
        data_name = "303_sets",
        url_lang = 'en'
                )

get_303_studies = GetMetaOperator(
        task_id = "Get_studies_for_food_prices",
        dag = dag,
        variable = 303,
        key = "badania",
        data_name = "303_studies",
        url_lang = 'en'
                )

get_303_acts = GetMetaOperator(
        task_id = "Get_legal_acts_for_food_prices",
        dag = dag,
        variable = 303,
        key = "akty-prawne",
        data_name = "303_acts",
        url_lang = 'en'
                )

get_303_departments = GetMetaOperator(
        task_id = "Get_gov_departments_for_food_prices",
        dag = dag,
        variable = 303,
        key = "jednostki",
        data_name = "303_departments",
        url_lang = 'en'
                )

get_307_facts = GetFactsOperator(
        task_id = "Get_facts_for_m2_price_median",
        dag = dag,
        url_param = "variable/variable-data-section?",
        variable = 307,
        section = 485,
        year_start = 2010,
        year_end = default_args['end_date'].year,
        period_start = 270,
        period_end = 273,
        page0 = 0,
        url_lang = 'en'
                )

get_400_facts = GetFactsOperator(
        task_id = "Get_facts_for_avg_monthly_wages",
        dag = dag,
        url_param = "variable/variable-data-section?",
        variable = 400,
        section = 2,
        year_start = 2010,
        year_end = default_args['end_date'].year,
        period_start = 270,
        period_end = 273,
        page0 = 0,
        url_lang = 'en'
                )

get_303_1_facts = GetFactsOperator(
        task_id = "Get_facts_for_food_prices_part_1",
        dag = dag,
        url_param = "variable/variable-data-section?",
        variable = 303,
        section = 740,
        year_start = 2010,
        year_end = 2016,
        period_start = 247,
        period_end = 258,
        page0 = 0,
        url_lang = 'en'
                )

get_303_2_facts = GetFactsOperator(
        task_id = "Get_facts_for_food_prices_part_2",
        dag = dag,
        url_param = "variable/variable-data-section?",
        variable = 303,
        section = 740,
        year_start = 2017,
        year_end = default_args['end_date'].year,
        period_start = 247,
        period_end = 258,
        page0 = 0,
        url_lang = 'en'
                )

create_tables_in_redshift = CreateTablesOperator(
    task_id="Create_tables",
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.dwh_tables_create
    )

load_area_dictionary = LoadToRedshiftOperator(
    task_id='Load_areas',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_areas",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_areas.csv"]  
    )

load_area_variables_dictionary = LoadToRedshiftOperator(
    task_id='Load_wage_variables_dictionaries',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_area_variables",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_wages_variables.csv","dictionaries/dict_real_estate_variables.csv","dictionaries/dict_retail_prices_variables.csv"]
    )

load_variables_dictionary = LoadToRedshiftOperator(
    task_id='Load_variables_dictionary',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_variables",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_variables.csv"]
    )

load_dims_positions_dictionary = LoadToRedshiftOperator(
    task_id='Load_dims_positions_dictionaries',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_dims_sections",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_dims_sections_307.csv","dictionaries/dict_dims_sections_400.csv","dictionaries/dict_dims_sections_303.csv"]
    )

load_pres_methodology_dictionary = LoadToRedshiftOperator(
    task_id='Load_presentation_methodology_dictionary',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_pres_methods",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_pres_methods.csv"]
    )

load_periods_dictionary = LoadToRedshiftOperator(
    task_id='Load_periods_dictionary',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_periods",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_periods.csv"]
    )

load_dates_dictionary = LoadToRedshiftOperator(
    task_id='Load_dates_dictionary',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_dates",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_dates.csv"]
    )

load_novalue_reason_dictionary = LoadToRedshiftOperator(
    task_id='Load_novalue_reason_dictionary',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_novalue_reason",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_novalue_reason.csv"]
    )

load_confidentiality_code_dictionary = LoadToRedshiftOperator(
    task_id='Load_confidentiality_code_dictionary',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_confidentiality_code",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_confidentiality_code.csv"]
    )

load_flags_dictionary = LoadToRedshiftOperator(
    task_id='Load_flags_dictionary',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_dict_flags",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["dictionaries/dict_flags.csv"]
    )

load_sections_metadata = LoadToRedshiftOperator(
    task_id='Load_sections_metadata',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sections",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_sections.csv","metadata/400/400_sections.csv","metadata/303/303_sections.csv"]
    )

load_terms_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_terms_metadata_307',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_terms_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_terms.csv"]
    )

load_terms_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_terms_metadata_400',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_terms_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_terms.csv"]
    )

load_terms_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_terms_metadata_303',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_terms_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_terms.csv"]
    )

load_departments_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_departments_metadata_307',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_departments_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_departments.csv"]
    )

load_departments_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_departments_metadata_400',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_departments_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_departments.csv"]
    )

load_departments_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_departments_metadata_303',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_departments_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_departments.csv"]
    )

load_methodology_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_methodology_metadata_307',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_methodology_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_methodology.csv"]
    )

load_methodology_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_methodology_metadata_400',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_methodology_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_methodology.csv"]
    )

load_methodology_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_methodology_metadata_303',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_methodology_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_methodology.csv"]
    )

load_sets_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_sets_metadata_307',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sets_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_sets.csv"]
    )

load_sets_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_sets_metadata_400',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sets_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_sets.csv"]
    )

load_sets_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_sets_metadata_303',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_sets_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_sets.csv"]
    )

load_studies_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_studies_metadata_307',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_studies_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_studies.csv"]
    )

load_studies_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_studies_metadata_400',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_studies_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_studies.csv"]
    )

load_studies_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_studies_metadata_303',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_studies_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_studies.csv"]
    )

load_tags_metadata_307 = LoadToRedshiftOperator(
    task_id='Load_tags_metadata_307',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_tags_307",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/307/307_tags.csv"]
    )

load_tags_metadata_400 = LoadToRedshiftOperator(
    task_id='Load_tags_metadata_400',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_tags_400",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/400/400_tags.csv"]
    )

load_tags_metadata_303 = LoadToRedshiftOperator(
    task_id='Load_tags_metadata_303',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_tags_303",
    truncate = True,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["metadata/303/303_tags.csv"]
    )

load_real_estate_logs = LoadToRedshiftOperator(
    task_id='Load_real_estate_logs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_realestate_prices",
    truncate = False,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["facts/307/"]
    )

load_wages_logs = LoadToRedshiftOperator(
    task_id='Load_wages_logs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_wages",
    truncate = False,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["facts/400/"]
    )

load_retail_prices_logs = LoadToRedshiftOperator(
    task_id='Load_retail_prices_logs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id = 'aws_credentials',
    table="staging_retail_prices",
    truncate = False,
    s3_bucket="gus-dbw-rawdata",
    s3_key=["facts/303/"]
    )

insert_into_areas_table = LoadTableOperator(
    task_id='Insert_data_into_areas_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="areas",
    sql=SqlQueries.areas_table_insert,
    truncate = True
    )

insert_into_variables_table = LoadTableOperator(
    task_id='Insert_data_into_variables_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="variables",
    sql=SqlQueries.variables_table_insert,
    truncate = True
    )

insert_into_sections_periods_table = LoadTableOperator(
    task_id='Insert_data_into_sections_periods_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="sections_periods",
    sql=SqlQueries.sections_periods_table_insert,
    truncate = True
    )

insert_into_dims_positions_table = LoadTableOperator(
    task_id='Insert_data_into_dims_positions_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="dims_positions",
    sql=SqlQueries.dims_positions_table_insert,
    truncate = True
    )

insert_into_pres_method_table = LoadTableOperator(
    task_id='Insert_data_into_pres_method_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="pres_method",
    sql=SqlQueries.pres_method_table_insert,
    truncate = True
    )

insert_into_periods_table = LoadTableOperator(
    task_id='Insert_data_into_periods_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="periods",
    sql=SqlQueries.periods_table_insert,
    truncate = True
    )

insert_into_dates_table = LoadTableOperator(
    task_id='Insert_data_into_dates_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="dates",
    sql=SqlQueries.dates_table_insert,
    truncate = True
    )

insert_into_novalue_reason_table = LoadTableOperator(
    task_id='Insert_data_into_novalue_reason_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="novalue_reason",
    sql=SqlQueries.novalue_reason_table_insert,
    truncate = True
    )

insert_into_confidentiality_code_table = LoadTableOperator(
    task_id='Insert_data_into_confidentiality_code_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="confidentiality_code",
    sql=SqlQueries.confidentiality_code_table_insert,
    truncate = True
    )

insert_into_flags_table = LoadTableOperator(
    task_id='Insert_data_into_flags_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="flags",
    sql=SqlQueries.flags_table_insert,
    truncate = True
    )

insert_into_sections_table = LoadTableOperator(
    task_id='Insert_data_into_sections_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="sections",
    sql=SqlQueries.sections_table_insert,
    truncate = True
    )

insert_307_into_terms_table = LoadTableOperator(
    task_id='Insert_307_data_into_terms_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="terms",
    sql=SqlQueries.terms307_table_insert,
    truncate = True
    )

insert_400_into_terms_table = LoadTableOperator(
    task_id='Insert_400_data_into_terms_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="terms",
    sql=SqlQueries.terms400_table_insert,
    truncate = False
    )

insert_303_into_terms_table = LoadTableOperator(
    task_id='Insert_303_data_into_terms_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="terms",
    sql=SqlQueries.terms303_table_insert,
    truncate = False
    )

insert_307_into_departments_table = LoadTableOperator(
    task_id='Insert_307_data_into_departments_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="departments",
    sql=SqlQueries.departments307_table_insert,
    truncate = True
    )

insert_400_into_departments_table = LoadTableOperator(
    task_id='Insert_400_data_into_departments_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="departments",
    sql=SqlQueries.departments400_table_insert,
    truncate = False
    )

insert_303_into_departments_table = LoadTableOperator(
    task_id='Insert_303_data_into_departments_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="departments",
    sql=SqlQueries.departments303_table_insert,
    truncate = False
    )

insert_307_into_methodology_table = LoadTableOperator(
    task_id='Insert_307_data_into_methodology_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="methodology",
    sql=SqlQueries.methodology307_table_insert,
    truncate = True
    )

insert_400_into_methodology_table = LoadTableOperator(
    task_id='Insert_400_data_into_methodology_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="methodology",
    sql=SqlQueries.methodology400_table_insert,
    truncate = False
    )

insert_303_into_methodology_table = LoadTableOperator(
    task_id='Insert_303_data_into_methodology_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="methodology",
    sql=SqlQueries.methodology303_table_insert,
    truncate = False
    )

insert_307_into_sets_table = LoadTableOperator(
    task_id='Insert_307_data_into_sets_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="sets",
    sql=SqlQueries.sets307_table_insert,
    truncate = True
    )

insert_400_into_sets_table = LoadTableOperator(
    task_id='Insert_400_data_into_sets_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="sets",
    sql=SqlQueries.sets400_table_insert,
    truncate = False
    )

insert_303_into_sets_table = LoadTableOperator(
    task_id='Insert_303_data_into_sets_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="sets",
    sql=SqlQueries.sets303_table_insert,
    truncate = False
    )

insert_307_into_studies_table = LoadTableOperator(
    task_id='Insert_307_data_into_studies_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="studies",
    sql=SqlQueries.studies307_table_insert,
    truncate = True
    )

insert_400_into_studies_table = LoadTableOperator(
    task_id='Insert_400_data_into_studies_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="studies",
    sql=SqlQueries.studies400_table_insert,
    truncate = False
    )

insert_303_into_studies_table = LoadTableOperator(
    task_id='Insert_303_data_into_studies_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="studies",
    sql=SqlQueries.studies303_table_insert,
    truncate = False
    )

insert_307_into_tags_table = LoadTableOperator(
    task_id='Insert_307_data_into_tags_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="tags",
    sql=SqlQueries.tags307_table_insert,
    truncate = True
    )

insert_400_into_tags_table = LoadTableOperator(
    task_id='Insert_400_data_into_tags_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="tags",
    sql=SqlQueries.tags400_table_insert,
    truncate = False
    )

insert_303_into_tags_table = LoadTableOperator(
    task_id='Insert_303_data_into_tags_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="tags",
    sql=SqlQueries.tags303_table_insert,
    truncate = False
    )

insert_wages_into_logs_table = LoadTableOperator(
    task_id='Insert_wages_data_into_logs_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="logs",
    sql=SqlQueries.logs_wages_table_insert,
    truncate = False
    )

insert_retail_prices_into_logs_table = LoadTableOperator(
    task_id='Insert_retail_prices_data_into_logs_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="logs",
    sql=SqlQueries.logs_retail_prices_table_insert,
    truncate = False
    )

insert_realestate_into_logs_table = LoadTableOperator(
    task_id='Insert_realestate_data_into_logs_table',
    dag=dag,
    redshift_conn_id='redshift',
    table="logs",
    sql=SqlQueries.logs_realestate_table_insert,
    truncate = False
    )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    dq_checks= [
            {'check_sql': "SELECT COUNT(*) FROM logs WHERE variable_id is null", 'expected_result': 0}
              ],
    tables = ['logs']
    )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
dict_operator = DummyOperator(task_id="Generic_dictionaries_done", dag=dag)
factmeta_operator = DummyOperator(task_id="Facts_metadata_done", dag=dag)
facts_properties_operator = PythonOperator(task_id="Properties_facts_done", dag=dag, python_callable=lambda: time.sleep(905))
facts_wages_operator = PythonOperator(task_id="Wages_facts_done", dag=dag, python_callable=lambda: time.sleep(905))
facts_goods_operator = PythonOperator(task_id="Goods_prices_facts_part-1_done", dag=dag, python_callable=lambda: time.sleep(905))
s3_operator = DummyOperator(task_id="Data_loaded_to_S3", dag=dag)
end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> get_areas
start_operator >> get_variables
start_operator >> get_dates
start_operator >> get_periods
start_operator >> get_pres_method
start_operator >> get_novalue_reason
start_operator >> get_confidentiality_code
start_operator >> get_flags
start_operator >> get_real_estate_variables
start_operator >> get_wages_variables
start_operator >> get_retail_prices_variables
start_operator >> get_307_dims_sections
start_operator >> get_400_dims_sections
start_operator >> get_303_dims_sections
get_areas >> dict_operator
get_variables >> dict_operator
get_dates >> dict_operator
get_periods >> dict_operator
get_pres_method >> dict_operator
get_novalue_reason >> dict_operator
get_confidentiality_code >> dict_operator
get_flags >> dict_operator
get_real_estate_variables >> dict_operator
get_wages_variables >> dict_operator
get_retail_prices_variables >> dict_operator
get_307_dims_sections >> dict_operator
get_400_dims_sections >> dict_operator
get_303_dims_sections >> dict_operator

dict_operator >> get_307_tags
dict_operator >> get_307_terms
dict_operator >> get_307_method
dict_operator >> get_307_sections
dict_operator >> get_307_sets
dict_operator >> get_307_studies
dict_operator >> get_307_acts
dict_operator >> get_307_departments
get_307_tags >> factmeta_operator
get_307_terms >> factmeta_operator
get_307_method >> factmeta_operator
get_307_sections >> factmeta_operator
get_307_sets >> factmeta_operator
get_307_studies >> factmeta_operator
get_307_acts >> factmeta_operator
get_307_departments >> factmeta_operator
dict_operator >> get_400_tags
dict_operator >> get_400_terms
dict_operator >> get_400_method
dict_operator >> get_400_sections
dict_operator >> get_400_sets
dict_operator >> get_400_studies
dict_operator >> get_400_acts
dict_operator >> get_400_departments
get_400_tags >> factmeta_operator
get_400_terms >> factmeta_operator
get_400_method >> factmeta_operator
get_400_sections >> factmeta_operator
get_400_sets >> factmeta_operator
get_400_studies >> factmeta_operator
get_400_acts >> factmeta_operator
get_400_departments >> factmeta_operator
dict_operator >> get_303_tags
dict_operator >> get_303_terms
dict_operator >> get_303_method
dict_operator >> get_303_sections
dict_operator >> get_303_sets
dict_operator >> get_303_studies
dict_operator >> get_303_acts
dict_operator >> get_303_departments
get_303_tags >> factmeta_operator
get_303_terms >> factmeta_operator
get_303_method >> factmeta_operator
get_303_sections >> factmeta_operator
get_303_sets >> factmeta_operator
get_303_studies >> factmeta_operator
get_303_acts >> factmeta_operator
get_303_departments >> factmeta_operator

factmeta_operator >> get_307_facts
get_307_facts >> facts_properties_operator
facts_properties_operator >> get_400_facts
get_400_facts >> facts_wages_operator
facts_wages_operator >> get_303_1_facts
get_303_1_facts >> facts_goods_operator
facts_goods_operator >> get_303_2_facts
get_303_2_facts >> s3_operator

s3_operator >> create_tables_in_redshift
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

insert_into_areas_table >> run_quality_checks
insert_into_variables_table >> run_quality_checks
insert_into_sections_periods_table >> run_quality_checks
insert_into_dims_positions_table >> run_quality_checks
insert_into_pres_method_table >> run_quality_checks
insert_into_periods_table >> run_quality_checks
insert_into_dates_table >> run_quality_checks
insert_into_novalue_reason_table >> run_quality_checks
insert_into_confidentiality_code_table >> run_quality_checks
insert_into_flags_table >> run_quality_checks
insert_into_sections_table >> run_quality_checks
insert_303_into_terms_table >> run_quality_checks
insert_303_into_departments_table >> run_quality_checks
insert_303_into_methodology_table >> run_quality_checks
insert_303_into_sets_table >> run_quality_checks
insert_303_into_studies_table >> run_quality_checks
insert_303_into_tags_table >> run_quality_checks
insert_realestate_into_logs_table >> run_quality_checks
run_quality_checks >> end_operator
