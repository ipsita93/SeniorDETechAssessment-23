#!/usr/bin/env python3

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum
import os

# from etl_scripts import etl_script


sg_tz = pendulum.timezone("Asia/Singapore")


os.environ["AIRFLOW_HOME"] = "~/airflow"
AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
input_dir = AIRFLOW_HOME + '/dags/etl_scripts/input/'
input_file_pattern = 'applications_dataset_*.csv'
successful_output_dir = AIRFLOW_HOME + '/dags/etl_scripts/applications_successful/'
unsuccessful_output_dir = AIRFLOW_HOME + '/dags/etl_scripts/applications_unsuccessful/'
archive_dir = AIRFLOW_HOME + '/dags/etl_scripts/archive/'

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 20, tzinfo=sg_tz),
    'email': ['ipsitamohapatra93@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def execute_etl_task (ti):
    input_csvs = ti.xcom_pull(task_ids=['check_input_csvs'])[0].split(' ')
    etl_script.main( input_csvs, successful_output_dir, unsuccessful_output_dir )


with DAG(
    "ipsita_hourly_script_run"
    , default_args=default_args
    , schedule_interval='05 * * * *'
    , catchup=False
    , tags=['ipsita']
) as dag:

    task_check_input = BashOperator(
        task_id="check_input_csvs",
        env={'INPUT_DIR': input_dir},
        bash_command="ls -l $INPUT_DIR/{input_file_pattern}; find $INPUT_DIR -name '{input_file_pattern}' | xargs".format(input_file_pattern=input_file_pattern),
        do_xcom_push=True
    )

    task_execute_etl = PythonOperator(
        task_id='execute_python_etl_code',
        python_callable=execute_etl_task,
        op_args=[],
        op_kwargs={}
    )
    
    task_archive_input = BashOperator(
        task_id="archive_input_csvs",
        env={"ARCH_PARENT_DIR": archive_dir},
        bash_command="ARCH_DIR=$ARCH_PARENT_DIR/input_$(date +'%Y%m%d_%H'); mkdir -p $ARCH_DIR; mv {{ ti.xcom_pull(task_ids='check_input_csvs') }} $ARCH_DIR/; ls -l $ARCH_DIR"
    )

    task_check_input >> task_execute_etl >> task_archive_input


