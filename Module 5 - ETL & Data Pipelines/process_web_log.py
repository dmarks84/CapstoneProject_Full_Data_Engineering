from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#define DAG arguments
default_args = {
    'owner': 'Dan Marks',
    'start_date': days_ago(0),
    'email': ['dmarks@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

# define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='ETL pipeline from web server log',
    schedule_interval=timedelta(days=1))

#define tasks
dest_path = '/home/project/airflow/dags/capstone'
source_file_name = 'accesslog.txt'
source_file = dest_path+'/'+source_file_name
extract_file_name = 'extracted_data.txt'
extract_file = dest_path+'/'+extract_file_name
extract_data = BashOperator(
    task_id="extract_data",
    bash_command=f"cat {source_file} \
        | cut -d'.' -f1,2,3,4 \
        | cut -d' ' -f1 > {extract_file}",
    dag=dag)

trans_file_name = 'transformed_data.txt'
trans_file = dest_path+'/'+trans_file_name
remove_ip = '198.46.149.143'
transform_data = BashOperator(
    task_id="transform_data",
    bash_command=f"cat {extract_file} \
        | sed 's/{remove_ip}//g' \
        | sed '/^[[:space:]]*$/d' > {trans_file}",
    dag=dag)

load_file_name = 'weblog.tar'
load_file = dest_path+'/'+load_file_name
load_data = BashOperator(
    task_id="load_data",
    bash_command=f"tar -C {dest_path} -cf {load_file} {trans_file_name}",
    dag=dag)

# task pipeline
extract_data >> transform_data >> load_data