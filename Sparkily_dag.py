from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


s3_bucket = "udacity-dend"
song_s3_key = "song_data"
log_s3_key = "log-data"
log_json_file = "log_json_path.json"

#AWS_KEY = os.environ.get('KEY')
#AWS_SECRET = os.environ.get('SECRET')

default_args = {
    'owner': "Thibaut",
    'start_date': datetime(2020, 4, 17),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes = 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False
}

dag_name = "Sparkily_dag"
dag = DAG(dag_name,
          default_args = default_args,
          description = "Load and transform data in Redshift with Airflow",
          schedule_interval = "0 * * * *"
          max_active_runs = 3
        )


start_operator = DummyOperator(task_id = "Begin_execution",  dag = dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_events",
    redshift_connect_id = "redshift-cluster1",
    aws_credentials = "aws_credentials",
    table = "staging_events",
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    file_format = "JSON",
    log_json_file = log_json_file,
    dag = dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_songs",
    redshift_connect_id = "redshift-cluster1",
    aws_credentials = "aws_credentials",
    table = "staging_songs",
    s3_bucket = s3_bucket,
    s3_key = song_s3_key,
    file_format = "JSON",
    log_json_file = log_json_file,
    dag = dag
)


load_songplays_table = LoadFactOperator(
    task_id = "Load_songplays_fact_table",
    redshift_connect_id = "redshift-cluster1",
    sql_query = "SqlQueries.songplay_table_insert",
    dag = dag
)


load_user_dimension_table = LoadDimensionOperator(
    task_id = "Load_user_dim_table",
    redshift_connect_id = "redshift-cluster1",
    sql_query = "SqlQueries.user_table_insert",
    table = "users"
    delete_load = True,
    dag = dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    redshift_connect_id = "redshift-cluster1",
    sql_query = "SqlQueries.song_table_insert",
    table = "songs"
    delete_load = True,
    dag = dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    redshift_connect_id = "redshift-cluster1",
    sql_query = "SqlQueries.artist_table_insert",
    table = "artists"
    delete_load = True,
    dag = dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    redshift_connect_id = "redshift-cluster1",
    sql_query = "SqlQueries.time_table_insert",
    table = "time"
    delete_load = True,
    dag = dag
)


run_quality_checks = DataQualityOperator(
    task_id = "Run_data_quality_checks",
    dag = dag,
    redshift_connect_id = "redshift-cluster1",
    tables = ["artists", "songplays", "songs", "time", "users"]
                                        )


end_operator = DummyOperator(task_id = 'Stop_execution',  dag = dag)


Begin_execution >> [Stage_events, Stage_songs] >> Load_songplays_fact_table
Load_songplays_fact_table >> [Load_song_dim_table, Load_user_dim_table, Load_artist_dim_table, Load_time_dim_table] >> Run_data_quality_checks
Run_data_quality_checks >> Stop_execution