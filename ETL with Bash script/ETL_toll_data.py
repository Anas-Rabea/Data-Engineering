from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow_task',
    'start_date': days_ago(0),
    'email': ['no_one@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    schedule_interval = timedelta(days=1),
    description = "Apache Airflow Final Assignment",
)

unzip_data = BashOperator(
    task_id='unzip_tar_file2',
    bash_command='tar -xvf /home/project/airflow/tolldata.tgz --directory $AIRFLOW_HOME/',
    dag = dag,
)


extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv_file',
    bash_command = 'cut -d "," -f1-4 $AIRFLOW_HOME/vehicle-data.csv \
        > $AIRFLOW_HOME/csv_data.csv',
        dag = dag,
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv_file',
    bash_command = 'cut -f5-7 $AIRFLOW_HOME/tollplaza-data.tsv| tr "\t" "," \
        > $AIRFLOW_HOME/tsv_data.csv',
    dag = dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width_file',
    bash_command = "awk '{print $10,$11}' $AIRFLOW_HOME/payment-data.txt | tr ' ' ',' \
        > $AIRFLOW_HOME/fixed_width_data.csv",
    dag = dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = "paste -d ','  $AIRFLOW_HOME/csv_data.csv  $AIRFLOW_HOME/fixed_width_data.csv $AIRFLOW_HOME/tsv_data.csv \
        > $AIRFLOW_HOME/extracted-data.csv",
    dag = dag,
)

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = "cut -d ',' -f1,4 $AIRFLOW_HOME/extracted-data.csv | tr '[:lower:]' '[:upper:]' \
        > $AIRFLOW_HOME/file_1.csv && cut --complement  -d ',' -f1,4 $AIRFLOW_HOME/extracted-data.csv \
            > $AIRFLOW_HOME/file_2.csv && paste -d ',' $AIRFLOW_HOME/file_1.csv $AIRFLOW_HOME/file_2.csv \
                > $AIRFLOW_HOME/transformed_data.csv",
    dag = dag,
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
