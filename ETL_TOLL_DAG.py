#Python libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import pendulum


local_tz=pendulum.timezone("Europe/Moscow")

# [START default_args]
default_args = {
   'owner': 'airflow',
   'start_date': datetime(2021,12,14,  tzinfo=local_tz),
   'email': ['dummy@dummy.com'],
   'email_on_failure': True,
   'email_on_retry': True,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}
# [START DAG]
with DAG(
   dag_id='ETL_toll_data',
   schedule_interval='@daily',
   description='Apache Airflow Final Assignment',
   default_args=default_args,
   ) as dag:

   unzip_commands = """
   rm -rf /home/airflow/tmp/*;
   wget -P /home/airflow/tmp https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz;
   tar -xvf /home/airflow/tmp/tolldata.tgz -C /home/airflow/tmp;
   """
   unzip_data = BashOperator(
                             task_id='unzip_task',
                             bash_command=unzip_commands,
                             )

   def extract_data_from_csv():
        data = pd.read_csv("/home/airflow/tmp/vehicle-data.csv",
                             usecols=[0,1,2,3],
                             names=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'])
        data.to_csv('/home/airflow/tmp/csv_data.csv', index=False)

   def extract_data_from_tsv():
        data = pd.read_csv("/home/airflow/tmp/tollplaza-data.tsv",
                        sep='\t',
                        usecols=[4,5,6],
                        names=['Number of axles', 'Tollplaza id', 'Tollplaza code'])
        data.to_csv('/home/airflow/tmp/tsv_data.csv', index=False)

   def extract_data_from_fixed_width():
        data = pd.read_csv("/home/airflow/tmp/payment-data.txt",
                        sep='\s+',
                        usecols=[10,11],
                        names=['Type of Payment code', 'Vehicle Code'])
        data.to_csv('/home/airflow/tmp/fixed_width_data.csv', index=False)

   def consolidate_data():
        data_csv=pd.read_csv('/home/airflow/tmp/csv_data.csv')
        data_tsv=pd.read_csv('/home/airflow/tmp/tsv_data.csv')
        data_fixed_width=pd.read_csv('/home/airflow/tmp/fixed_width_data.csv')
        df=pd.concat([data_csv, data_tsv, data_fixed_width], axis=1)
        df.to_csv('/home/airflow/tmp/extracted_data.csv', index=False)

   def transform_data():
       data=pd.read_csv('/home/airflow/tmp/extracted_data.csv')
       data['Vehicle type'] = data['Vehicle type'].str.upper()
       data.to_csv('/home/airflow/tmp/transformed_data.csv', index=False)

   extract_data_from_csv = PythonOperator (
       task_id='extract_data_from_csv',
       python_callable=extract_data_from_csv,
       )

   extract_data_from_tsv = PythonOperator (
       task_id='extract_data_from_tsv',
       python_callable=extract_data_from_tsv,
       )

   extract_data_from_fixed_width = PythonOperator (
       task_id='extract_data_from_fixed_width',
       python_callable=extract_data_from_fixed_width,
       )

   consolidate_data = PythonOperator (
       task_id='consolidate_data',
       python_callable=consolidate_data,
       )

   transform_data = PythonOperator (
       task_id='transform_data',
       python_callable=transform_data,
       )

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
