from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries
from subdag import load_dimensional_tables_dag


# start_date = datetime.utcnow()

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
#     'end_date': datetime(2019, 11, 30),
    'depends_on_past': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag_name='new_dag_24'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    jsonpath = 's3://udacity-dend/log_json_path.json',
#     execution_date=start_date
)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    region="us-west-2",
    data_format="JSON",
    jsonpath='auto',
#     execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table_task_id='Load_user_dim_table'
load_user_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_user_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2019, 1, 12),
        table="users",
#         truncate = 'True',
        sql_query=SqlQueries.user_table_insert,
    ),
    task_id=load_user_dimension_table_task_id,
    dag=dag,
)

load_song_dimension_table_task_id='Load_songs_dim_table'
load_song_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_song_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date=datetime(2019, 1, 12),
        table="songs",
        sql_query=SqlQueries.song_table_insert,
    ),
    task_id=load_song_dimension_table_task_id,
    dag=dag,
)

load_artist_dimension_table_task_id='Load_artist_dim_table'
load_artist_dimension_table = SubDagOperator(
      subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_artist_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artists",
        start_date= datetime(2019, 1, 12),
        sql_query=SqlQueries.artist_table_insert,
    ),
    task_id=load_artist_dimension_table_task_id,
    dag=dag,
)

load_time_dimension_table_task_id='Load_time_dim_table'
load_time_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_time_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="time",
        start_date=datetime(2019, 1, 12),
        sql_query=SqlQueries.time_table_insert,
    ),
    task_id=load_time_dimension_table_task_id,
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting tasks dependencies

start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
