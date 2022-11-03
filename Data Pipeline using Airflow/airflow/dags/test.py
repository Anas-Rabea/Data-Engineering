from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)
from operators import StageToRedshiftOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AKIA524HCXRFQXTFJD2Q')
AWS_SECRET = os.environ.get('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')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
#     'email_on_retry': False,
#     'retries': 3,
#     'catchup':False,
#     'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    redshift_conn_id = "redshift",
    table = "staging_events",
    aws_credentials = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_json_path.json",
    task_id='Stage_events',
    dag=dag,
    
)

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag
# )

# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>stage_events_to_redshift>>end_operator