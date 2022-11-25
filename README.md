# 1. PURPOSE

Polish Statistics Office collects and provides socio-economics and environmental data both on their website as well as
via Knowledge Database (DBW), enabling access with REST API and returning JSON or CSV format.
Where API approach (https://api-dbw.stat.gov.pl/apidocs/index.html?urls.primaryName=API%20DBW%20(en%201.1.0)) 
brings significant simplification comparing to web-browsing, it still requires numerous requests
in order to pull needed facts and additional metadata describing collected numbers. 
As a result, user receives high number of separated files (json/csv) or dataframes lacking an organized schema allowing
for aggregations and further analytics.

One of the options for potential improvement is to move the data into datawarehouse.
Such approach was applied in this excercise. 
For 3 categories:
- median price of 1m2 in Poland (variable_id = 307)
- average gross wages in Poland (variable_id = 400)
- average retail prices of food and consumer services (variable_id=303)
and period between 2010 - today, values and their respective metadata are going to be collected into S3 bucket structure
and then transferred into AWS Redshift, building there a star-schema.

Due to the quantity of API requests and constant updates in DBW, the entire process is orchestrated by Apache Airflow.
The idea is to pull the data on 10th day of each month.

This strategy opens a possibility for:
- easy scaling up, since increase of data by let's say 100x does not make any difference (other than aws costs)
- changes in frequency of call as pipeline can be run as per the user requirements

However... there is one serious limitation coming from API DBW:  
up to 5 requests per second, 100 requests per 15 minutes, 1,000 requests per 12 hours, 10,000 requests per 7 days.

To deal with it, due to large quantity of requests during the first run, python delay operators were installed into the dag, 
as well as small delays into the get_facts, get_data and get_meta operators.
This is mainly a solution for "5 per second" and "100 per 15 minutes" limits.
It does not change much if dag operates once per month but it's good to keep it in mind.
Another workaround could be to split the requests between more than 1 user.

# 2. MANUAL

Apache Airflow is run in the Docker container. 
After Airflow is initiated in Docker, http://localhost:8080/ can be accessed via browser and gus_dag becomes available.
Airflow version 2.4.2 provides all required packages.
Furthermore, connections to AWS need to be established and declared in Airflow.
This goes for:
- aws_credentials with access and secret key
- s3_conn that's basically the same as above however in the meantime S3 conn_type got deprecated in Airflow
- redshift with rs credentials

# 3. STRUCTURE

The repository consists of the following:

## 3.1 dags

gus_dag.py:
Airflow dag orchestrating the entire pipeline:
- API requests to DBW
- colletion of facts, dictionaries and metadata into S3 bucket structure
- building staging tables and star schema in Redshift
- populating staging tables from S3 csv files
- loading final schema from staging

## 3.2 operators/plugins

get_facts.py - API calls for real estate, wages, retail prices information, into S3 bucket
get_data.py - API calls for general metadata dictionary, into S3 bucket
get_meta.py - API calls for specific metadata related to each category, into S3 bucket
create_tables.py - creates all Redshift tables, staging and final schema
load_redshift.py - populates staging tables from csv files located in S3 bucket
load_table.py - loads final schema from staging tables
run_quality_checks.py - DataQualityOperator verifying data integrity in (currently) logs table searching for:
                        nulls in variable_id, empty table

## 3.3 operators/helpers

sql_queries.py - collection of sql queries for create_tables.py, load_redshift.py and load_table.py,
                 all that's required to manipulate Redshift.


## 3.4 docker-compose.yaml

For Apache Airflow docker run.

# 4. SCHEMA & PIPELINE

Redshift database Schema is of a Star type, providing best results for OLAP.
This architecture shall allow for fast aggregations and simple queries (also those not yet foreseen).
Small deviation are:
- "areas" table connected to "variables" via area_id 
- "sections_periods" table connected to "sections" via section_id
Both are actually not required for operations within only this schema but provide an overview on other categories,
in case the user would like to explore those as well.

## 4.1 Staging Dataframes
Before the data lands in the final tables, it is initially loaded inside the staging dataframes:
- staging_dict_areas
- staging_dict_area_variables
- staging_dict_variables        
- staging_dict_dims_sections
- staging_dict_pres_methods
- staging_dict_periods
- staging_dict_dates
- staging_dict_novalue_reason
- staging_dict_confidentiality_code
- staging_dict_flags
- staging_sections
- staging_terms_307
- staging_terms_400
- staging_terms_303
- staging_departments_307
- staging_departments_400
- staging_departments_303
- staging_methodology_307
- staging_methodology_400
- staging_methodology_303
- staging_sets_307
- staging_sets_400
- staging_sets_303
- staging_studies_307
- staging_studies_400
- staging_studies_303
- staging_tags_307
- staging_tags_400
- staging_tags_303
- staging_wages
- staging_retail_prices
- staging_realestate_prices

Details of all tables can be investigate in sql_queries.py.

## 4.2 Final Schema

The final schema consists of:
a) Fact table: logs <= staging_wages + staging_retail_prices + staging_realestate_prices

b) Dimension tables based on API DBW dictionaries:
- areas <= staging_dict_areas
- variables <= staging_dict_area_variables
- sections_periods <= staging_dict_variables
- dims_positions <= staging_dict_dims_sections
- pres_methods <= staging_dict_pres_methods
- periods <= staging_dict_periods
- dict_dates <= staging_dict_dates
- novalue_reason <= staging_dict_novalue_reason
- confidentiality_code <= staging_dict_confidentiality_code
- flags <= staging_dict_flags

c) Dimension tables based on API DWB metadata:
- sections <= staging_sections
- terms <= staging_terms_307 + staging_terms_400 + staging_terms_303
- departments <= staging_departments_307 + staging_departments_400 + staging_departments_303
- methodology <= staging_methodology_307 + staging_methodology_400 + staging_methodology_303
- sets <= staging_sets_307 + staging_sets_400 + staging_sets_303
- studies <= staging_studies_307 + staging_studies_400 + staging_studies_303
- tags <= staging_tags_307 + staging_tags_400 + staging_tags_303

Details of all tables can be investigate in sql_queries.py.
