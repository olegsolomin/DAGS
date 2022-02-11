
# Python libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import time
import pendulum
import mariadb
import sys
sys.path.append('/home/fmsb/fmsb/scripts')
import fmsb
import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET

#variables
secrets = fmsb.secrets()
date_x = time.strftime("%d/%m/%Y")
date = time.strftime("%Y/%m/%d")
address = 'http://www.cbr.ru/scripts/XML_daily.asp?date_req='+date_x
local_tz = pendulum.timezone("Europe/Moscow")

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2021,12,9, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# TASK definition
def create_table_cbratetoday():
    try:
        conn = mariadb.connect(host=secrets['host'],
            port=secrets['port'],
            database=secrets['database'],
            user=secrets['user'],
            password=secrets['pass'],
            connect_timeout=3)
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS cbratetoday")
    cur.execute('''CREATE TABLE cbratetoday (id serial, date DATE, sid TEXT, numcode INTEGER, charcode TEXT, name TEXT UNIQUE, nominal INTEGER, value FLOAT, updated_at TIMESTAMP, primary key (id));''')

def load_data_cbratetoday():
    data = urllib.request.urlopen(address)
    stuff = ET.parse(data)
    all = stuff.findall('.//Valute')
    try:
        conn = mariadb.connect(host=secrets['host'],
            port=secrets['port'],
            database=secrets['database'],
            user=secrets['user'],
            password=secrets['pass'],
            connect_timeout=3)
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    cur = conn.cursor()
    for entry in all:
        sid = entry.get('ID')
        charcode=entry.find('CharCode').text
        numcode=entry.find('NumCode').text
        name=entry.find('Name').text
        nominal=entry.find('Nominal').text
        value=entry.find('Value').text.replace(',','.')
        updated_at=datetime.datetime.now()
        cur.execute('''INSERT INTO cbratetoday (date, sid, numcode, charcode, name, nominal, value, updated_at) VALUES ( %s,%s,%s,%s,%s,%s,%s, %s );''', (date, sid, numcode, charcode, name, nominal,value, updated_at) )
    conn.commit()
    cur.close()

# DAG definition
with DAG('cb_today',
    description='A simple DAG made by SON to update TODAYs rates of CBR',
    default_args=default_args,
    tags=['cbr', 'finmarket'],
    schedule_interval="45 */12 * * *") as dag:
    create_table_cbratetoday = PythonOperator(
        task_id='create_table_cbratetoday',
        python_callable=create_table_cbratetoday,
        dag=dag
    )
    load_data_cbratetoday = PythonOperator(
        task_id='load_data_cbratetoday',
        python_callable=load_data_cbratetoday,
        dag=dag
    )

# TASK pipeline
create_table_cbratetoday >> load_data_cbratetoday
