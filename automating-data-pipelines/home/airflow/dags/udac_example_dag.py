from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

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
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag, 
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_format="s3://udacity-dend/log_json_path.json"
)

# song_data/A/A/A

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_format="auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    conn_id = 'redshift',
    sql = SqlQueries.songplay_table_insert,
    table = 'songplays',
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag, 
    conn_id = 'redshift',
    sql = SqlQueries.user_table_insert,
    table = 'users',
    truncate_insert = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.song_table_insert,
    table = 'songs',
    truncate_insert = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.artist_table_insert,
    table = 'artists',
    truncate_insert = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id = 'redshift',
    sql = SqlQueries.time_table_insert,
    table = 'time',
    truncate_insert = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = 'redshift',
    checks=[
        {
            'type': 'sql',
            'query': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'result': 0
        },
        {
            'type': 'sql',
            'query': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL;',
            'op': 'eq',
            'result': 0
        },
        {
            'type': 'sql',
            'query': 'SELECT COUNT(*) FROM users;',
            'op': 'gt',
            'result': 0
        }
        ]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)




start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator