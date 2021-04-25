from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (
    PostgresOperator
)

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Andre Araujo',
    'start_date': datetime(2021, 4, 19),
    'depends_on_past': False,
    'email': ['dedeco@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
}

dag = DAG('udac_create_table_sparkify',
          default_args=default_args,
          description='Create tables facts, dimension and staging tables for sparkify',
          schedule_interval="@once",
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_table
create_table >> end_operator