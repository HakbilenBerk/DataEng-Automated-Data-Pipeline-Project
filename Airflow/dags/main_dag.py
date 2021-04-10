from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'BerkHakbilen',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=1),
    'catchup' : False,
    'email_on_retry' : False   
}


dag = DAG('main_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

'''
stage_json_s3 = PythonOperator(
    task_id = "convert_to_json_on_s3"
    python_callable =  convert_to_json_main,
    op_kwargs={'output_data':"s3a://dataeng-capstone/"}
    dag=dag,
)
'''

create_tables = PostgresOperator(
                task_id = "create_tables",
                dag = dag,
                postgres_conn_id = "redshift",
                sql = "create_tables.sql"               
)


stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_immigration_data',
    dag = dag,
    s3_bucket = "dataeng-capstone",
    s3_prefix = "immigration",
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "staging_immigration",
    region = "us-west-2"   
)

stage_temperature_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_temperature_data',
    dag = dag,
    s3_bucket = "dataeng-capstone",
    s3_prefix = "temperature",
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "staging_temperature",
    region = "us-west-2" 
)

stage_demographics_to_redshift = StageToRedshiftOperator(
    task_id = 'stage_demographics_data',
    dag = dag,
    s3_bucket = "dataeng-capstone",
    s3_prefix = "demogprahics",
    redshift_conn_id = "redshift",
    aws_credentials = "aws_credentials",
    table = "staging_demographics",
    region = "us-west-2" 
)

load_immigration_fact_table = LoadFactOperator(
    task_id = 'load_immigration_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "immigration",
    sql = SqlQueries.immigration_table_insert
)

load_temperature_dimension_table = LoadDimensionOperator(
    task_id = 'Load_temp_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "temperature",
    sql = SqlQueries.temperature_table_insert
)

load_demographics_dimension_table = LoadDimensionOperator(
    task_id = 'Load_demo_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "demographics",
    sql = SqlQueries.demographics_table_insert
)

load_visa_dimension_table = LoadDimensionOperator(
    task_id = 'Load_visa_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "visa_details",
    sql = SqlQueries.visa_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    sql = "SELECT COUNT(*) FROM {} WHERE {} IS NULL",
    result = 0,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >>  create_tables

create_tables >> stage_immigration_to_redshift
create_tables >> stage_temperature_to_redshift
create_tables >> stage_demographics_to_redshift

stage_immigration_to_redshift >> load_immigration_fact_table
stage_temperature_to_redshift >> load_immigration_fact_table
stage_demographics_to_redshift >> load_immigration_fact_table

load_immigration_fact_table >> load_temperature_dimension_table
load_immigration_fact_table >> load_demographics_dimension_table
load_immigration_fact_table >> load_visa_dimension_table


load_temperature_dimension_table >> run_quality_checks
load_demographics_dimension_table >> run_quality_checks
load_visa_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
