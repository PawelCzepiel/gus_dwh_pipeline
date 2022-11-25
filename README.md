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

To summarize, answers to 3 possible scenarios:

1) The data was increased by 100x.

There is no constrain coming from the pipeline itself.
API DBW quantity of possible requests is the only limitation.
Other than that, it can be 100x.

2) The pipelines would be run on a daily basis by 7 am every day.

Again, it could with Apache Airflow and AWS S3+Redshift,
as long as the limit of API DBW calls is not exceeded.

3) The database needed to be accessed by 100+ people.

AWS Redshift easily allow for that when the credentials are shared and set up.

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

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| log_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| variable_id | bigint | 8 Bytes | - | Variable Id | variables.variable_id |
| section_id | bigint | 8 Bytes | - | Section Id | sections.section_id |
| dim1_id | bigint | 8 Bytes | - | First Dimension Id | dims_positions.dim_id |
| position1_id | bigint | 8 Bytes | - | First Position Id | dims_positions.position_id |
| dim2_id | bigint | 8 Bytes | - | Second Dimension Id | dims_positions.dim_id|
| position2_id | bigint | 8 Bytes | - | Second Position Id | dims_positions.position_id |
| period_id | bigint | 8 Bytes | - | Period Id | periods.period_id |
| presmethodunit_id | bigint | 8 Bytes | - | Unit of Presentation Method Id | pres_methods.presmethodunit_id |
| year_id | bigint | 8 Bytes | - | Year | dates.year_key |
| novalue_id | bigint | 8 Bytes | - | Explanation of missing value Id | novalue_reason.novalue_id |
| confidentiality_id | bigint | 8 Bytes | - | Confidentially code Id | confidentiality_code.confidentiality_id |
| flag_id | bigint | 8 Bytes | - | Flag code Id | flags.flag_id |
| value | float8 | 8 Bytes | - | Recorded value per variable type | N/A |
| precison | bigint | 8 Bytes | - | Value precision | N/A |

b) Dimension tables based on API DBW dictionaries:
- areas <= staging_dict_areas

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| area_key | bigint | 8 Bytes | NOT NULL | name of the area parent to variable | variables.area_id |
| area_name | varchar | 256 | - | name of the area parent to variable | N/A |
| leaditem_id | float8 | 8 Bytes | - | id of the area parent to subject area | N/A |
| level_id | bigint | 8 Bytes | - | id of the level for subject area | N/A |
| level_name | varchar | 256 | - | name of the level for subject area | N/A |
| variables | boolean | 2 Bytes | - | whether area contains variables | N/A |

- variables <= staging_dict_area_variables

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| areavar_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| variable_id | bigint | 8 Bytes | NOT NULL | Variable Id | logs.variable_id,terms.variable_id,departments.variable_id,methodolody.variable_id,sets.variable_id,studies.variable_id,tags.variable_id |
| variable_name | varchar | Max | - | Variable Name | N/A |
| area_id | bigint | NOT NULL | - | Variable Name | areas.area_key |

- sections_periods <= staging_dict_variables

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| secper_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| section_id | bigint | 8 Bytes | PRIMARY KEY | Section Id | logs.section_id |
| section_name | varchar | Max | - | Section Name, variable sub-category | N/A |
| period_id | bigint | 8 Bytes | PRIMARY KEY | Period Id | N/A |
| variable_id | bigint | 8 Bytes | NOT NULL | Variable Id | N/A |

- dims_positions <= staging_dict_dims_sections

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| dimpos_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| dim_id | bigint | 8 Bytes | PRIMARY KEY | Dimension Id | logs.dim_id |
| dim_name | varchar | 256 | - | Dimension Name, data category | N/A |
| position_id | bigint | 8 Bytes | PRIMARY KEY | Position Id | logs.position_id |
| position_name | varchar | 256 | - | Position Name, data sub-category to dimension | N/A |
| section_id | bigint | 8 Bytes | - | Section Id | N/A |

- pres_methods <= staging_dict_pres_methods

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| presmethod_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| presmethodunit_id | bigint | 8 Bytes | NOT NULL | Id of Presentation Method in terms of measurement | logs.presmethodunit_id |
| presmethodunit_name | varchar | max | - | Name of Presentation Method in terms of measurement | N/A |
| presmethod_name | varchar | max | - | Description of Presentation Method | N/A |
| unit_id | bigint | 8 Bytes | - | Measurement Unit Id | N/A |
| unit_description | varchar | 256 | - | Measurement Unit Description | N/A |
| unit_name | varchar | 256 | - | Measurement Unit Name | N/A |

- periods <= staging_dict_periods

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| period_key | bigint | 8 Bytes | PRIMARY KEY | Period Id | logs.period_id |
| symbol | varchar | 256 | - | Period Symbol | N/A |
| description | varchar | 256 | - | Period Description | N/A |
| frequency_id | bigint | 8 Bytes | - | Period Frequency Id | N/A |
| frequency_name | varchar | 256 | - | Period Frequency Name | N/A |
| type_id | bigint | 8 Bytes | - | Period Type Id | N/A |
| type_name | varchar | 256 | - | Period Type Name | N/A |

- dict_dates <= staging_dict_dates

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| year_key | bigint | 8 Bytes | PRIMARY KEY | Year | logs.year_id |

- novalue_reason <= staging_dict_novalue_reason

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| novalue_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| novalue_id | bigint | 8 Bytes | NOT NULL | Explanation of missing value Id | logs.novalue_id |
| mark | varchar | 256 | - | Explanation of missing value Mark | N/A |
| name | varchar | max | - | Explanation of missing value | N/A |

- confidentiality_code <= staging_dict_confidentiality_code

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| confidentiality_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| confidentiality_id | bigint | 8 Bytes | NOT NULL | Confidentiality type Id | logs.confidentiality_id |
| mark | varchar | 256 | - | Confidentiality type Mark | N/A |
| name | varchar | 256 | - | Confidentiality type description | N/A |

- flags <= staging_dict_flags

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| flag_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| flag_id | bigint | 8 Bytes | NOT NULL | Data Flag Id | logs.flag_id |
| mark | varchar | 256 | - | Data Flag Mark | N/A |
| name | varchar | 256 | - | Data Flag description | N/A |

c) Dimension tables based on API DWB metadata:
- sections <= staging_sections

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| section_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| section_id | bigint | 8 Bytes | - | Section Id | logs.section_id |
| section_name | varchar | 256 | - | Section Name, variable sub-category | N/A |
| timerange | varchar | 256 | - | Time range | N/A |
| frequency_id | bigint | 8 Bytes | - | Frequency Id for the Section | N/A |
| frequency_name | varchar | 256 | - | Frequency Name for the Section | N/A |
| update_last | varchar | 256 | - | Last update time | N/A |
| update_next | varchar | 256 | - | Next update time | N/A |

- terms <= staging_terms_307 + staging_terms_400 + staging_terms_303

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| term_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| term_id | bigint | 8 Bytes | NOT NULL | Term Id for Variable | N/A |
| term_name | varchar | max | - | Term Name for Variable | N/A |
| definition | varchar | max | - | Term Definition for Variable | N/A |
| remarks | varchar | max | - | Remarks for the Term | N/A |
| date_start | varchar | 256 | - | Date Start for the Term applicability | N/A |
| date_end | varchar | 256 | - | Date End for the Term applicability | N/A |
| variable_id | bigint | 8 Bytes | - | Variable Id | variables.variable_id |

- departments <= staging_departments_307 + staging_departments_400 + staging_departments_303

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| department_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| department_id | bigint | 8 Bytes | NOT NULL | Data Collection Department Id for Variable | N/A |
| department_name | varchar | 256 | - | Data Collection Department Name for Variable | N/A |
| variable_id | bigint | 8 Bytes | - | Variable Id | variables.variable_id |

- methodology <= staging_methodology_307 + staging_methodology_400 + staging_methodology_303

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| method_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| method_definition | varchar | max | - | Data Collection Methodology | N/A |
| variable_id | bigint | 8 Bytes | - | Variable Id | variables.variable_id |

- sets <= staging_sets_307 + staging_sets_400 + staging_sets_303

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| set_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| set_id | bigint | 8 Bytes | NOT NULL | Data Collection Set Id for Variable | N/A |
| set_symbol | varchar | 256 | - | Data Collection Set Symbol for Variable | N/A |
| set_name | varchar | max | - | Data Collection Set Name for Variable | N/A |
| admin_id | bigint | 8 Bytes | - | Data Collection Governing Body Id for Variable | N/A |
| admin_name | varchar | 256 | - | Data Collection Governing Body Name for Variable | N/A |
| variable_id | bigint | 8 Bytes | - | Variable Id | variables.variable_id |

- studies <= staging_studies_307 + staging_studies_400 + staging_studies_303

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| study_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| study_id | bigint | 8 Bytes | NOT NULL | Data Collection Study Id for Variable | N/A |
| study_symbol | varchar | 256 | - | Data Collection Study Symbol for Variable | N/A |
| study_subject | varchar | max | - | Data Collection Study Subject for Variable | N/A |
| variable_id | bigint | 8 Bytes | - | Variable Id | variables.variable_id |

- tags <= staging_tags_307 + staging_tags_400 + staging_tags_303

| Column_name | Data_type | Field_length | Constraint | Description | Foreign key & table |
| :---: | :---: | :---: | :---: | :---: | :---: |
| tag_key | bigint | 8 Bytes | identity(0,1) | index | N/A |
| tag_id | bigint | 8 Bytes | NOT NULL | Data Collection Tag Id for Variable | N/A |
| tag_name | varchar | max | - | Data Collection Tag Name for Variable | N/A |
| variable_id | bigint | 8 Bytes | - | Variable Id | variables.variable_id |

Details of all tables can be investigate in sql_queries.py.

## 5. Final ETL result and evidence

Query_1 : 
SELECT v.variable_name, s.section_name, d.year_key, p.description, count(log_key) FROM logs l
JOIN variables v ON v.variable_id = l.variable_id
JOIN sections s ON s.section_id = l.section_id
JOIN periods p ON p.period_key = l.period_id
JOIN dates d ON d.year_key = l.year_id
GROUP BY (v.variable_name, s.section_name, d.year_key, p.description)

Result_1: result_1.csv

This results shows total current number of records in logs fact table ~3.4 million. 

Query_2 :
SELECT v.variable_name, d.year_key, p.description, avg(l.value) FROM logs l
JOIN variables v ON v.variable_id = l.variable_id
JOIN sections s ON s.section_id = l.section_id
JOIN periods p ON p.period_key = l.period_id
JOIN dates d ON d.year_key = l.year_id
WHERE l.variable_id = 400
GROUP BY (v.variable_name, s.section_name, d.year_key, p.description)

Result_2: result_2.csv