from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_data="log-data",
        jsonPath="s3://udacity-dend/log_json_path.json"
    )

    start_operator >> stage_events_to_redshift

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_data="song-data",
        jsonPath="auto"
    )

    start_operator >> stage_songs_to_redshift

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="dim_users",
        table_fields="userid, firstname, lastname, gender, level",
        sql=final_project_sql_statements.SqlQueries.user_table_insert
    )

    load_songplays_table >> load_user_dimension_table

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="dim_songs",
        table_fields="song_id, title, artist_id, year, duration",
        sql=final_project_sql_statements.SqlQueries.song_table_insert
    )

    load_songplays_table >> load_song_dimension_table

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="dim_artists",
        table_fields="artist_id, artist_name, artist_location, artist_latitude, artist_longitude",
        sql=final_project_sql_statements.SqlQueries.artist_table_insert
    )

    load_songplays_table >> load_artist_dimension_table

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="dim_time",
        table_fields="""start_time, hour, day, week, month, year, dayofweek""",
        sql=final_project_sql_statements.SqlQueries.time_table_insert
    )

    load_songplays_table >> load_time_dimension_table

    run_data_quality_checks = DataQualityOperator(
        task_id='Check_for_artist_with_incomplete_geolocation',
        redshift_conn_id="redshift",
        checks=[
            "SELECT COUNT(*) FROM dim_users WHERE userid IS NULL", 
            """SELECT COUNT(*) FROM dim_artists 
                WHERE (artist_latitude IS NULL AND artist_latitude IS NOT NULL) 
                   OR (artist_latitude IS NOT NULL AND artist_latitude IS NULL)""",
            "SELECT COUNT(*) FROM dim_time WHERE start_time IS NULL",
            "SELECT COUNT(*) FROM dim_songs WHERE song_id IS NULL"],
        expected_results=[0, 0, 0, 0]
    )

    load_user_dimension_table >> run_data_quality_checks
    load_song_dimension_table >> run_data_quality_checks
    load_artist_dimension_table >> run_data_quality_checks
    load_time_dimension_table >> run_data_quality_checks

    end_operator = DummyOperator(task_id='End_execution')

    run_data_quality_checks >> end_operator

final_project_dag = final_project()