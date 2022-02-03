from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables= PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    table="stage_events",
    s3_bucket="udacity_dend",
    s3_key="log_data",
    copy_json_option="s3://udacity-dend/log_json_path.json",
    region="us_west_2",
    is_truncate = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    copy_json_option='auto',
    region='us-west-2',
    is_truncate = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    select_sql=SqlQueries.songplay_table_insert,
    is_truncate = True

)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    select_sql=SqlQueries.user_table_insert,
    append_insert=True,
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    select_sql=SqlQueries.song_table_insert,
    append_insert=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    select_sql=SqlQueries.artist_table_insert,
    append_insert=True,
    primary_key="artistid",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    select_sql=SqlQueries.time_table_insert,
    append_insert=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    test_results='select count(*) from songs where songid is null;',
    expected_results=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables
create_tables >> stage_events_to_redshift 
stage_events_to_redshift >> load_songplays_table
create_tables >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table 
load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table 
load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table 
load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table 
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator