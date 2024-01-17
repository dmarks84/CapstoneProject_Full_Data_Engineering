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
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

#define tasks
unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command="tar -zxvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging/",
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command="cut -d',' -f 1-4 vehicle-data.csv > csv_data.csv",
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command="tr '\t' ',' < tollplaza-data.tsv | cut -d',' -f 5-7 | tr -d '\r' > tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command="cat payment-data.txt | tr -s [:blank:] ',' | cut -d',' -f 11,12 > fixed_width_data.csv",
    dag=dag,
)

consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command="paste -d',' csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv",
    dag=dag,
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command="cat extracted_data.csv | sed 's/car/CAR/g' | sed 's/truck/TRUCK/g' | sed 's/van/VAN/g' > transformed_data.csv",
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data