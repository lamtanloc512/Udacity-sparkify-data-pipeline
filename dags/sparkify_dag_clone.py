from datetime import datetime, timedelta
import os

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (CreateTablesOperator, StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
# from plugins.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)
# from plugins.helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Lucas Lam',
    'start_date': pendulum.now(),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'depends_on_past': False
}


with DAG('sparkify_dag_clone',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='0 * * * *'
         ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    # Define the path to the SQL file
    sql_file_path = '/sql/create_tables.sql'

    create_table = CreateTablesOperator(
        task_id='Creating_tables',
        postgres_conn_id='redshift',
        sql_file=sql_file_path
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credientials_id='aws_credentials',
        redshift_conn_id='redshift',
        table='staging_events',
        s3_bucket='lucas-lam',
        s3_path='log_data',
        json_path='auto'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credientials_id='aws_credentials',
        redshift_conn_id='redshift',
        table='staging_songs',
        s3_bucket='lucas-lam',
        s3_path='song_data',
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        SQLquery=SqlQueries.songplay_table_insert,
        Truncate=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        SQLquery=SqlQueries.user_table_insert,
        table='users',
        Truncate=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        SQLquery=SqlQueries.song_table_insert,
        table='songs',
        Truncate=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        SQLquery=SqlQueries.artist_table_insert,
        table='artists',
        Truncate=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        SQLquery=SqlQueries.time_table_insert,
        table='time',
        Truncate=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

# Dependencies
start_operator >> create_table
create_table >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
    load_time_dimension_table] >> run_quality_checks >> end_operator
run_quality_checks >> end_operator
