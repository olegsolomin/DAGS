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
local_tz=pendulum.timezone("Europe/Moscow")
address = 'http://www.cbr.ru/scripts/XML_daily.asp?date_req='+date_x
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

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2021,12,9, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=3),
}

# TASK definition
def create_table_cbratedaily():
    cur.execute('''CREATE TABLE IF NOT EXISTS cbratedaily (id serial, date DATE,
    sid TEXT, numcode INTEGER, charcode TEXT, name TEXT,
    nominal INTEGER, value FLOAT, updated_at TIMESTAMP, primary key (id));''')

def load_data_cbratedaily():
    cur.execute('''SELECT DISTINCT date FROM cbratedaily''')
    dates=cur.fetchall()
    dates_n=[]
    for i in dates:
        dates_n.append(i[0].strftime('%Y-%m-%d'))

    if dates is None:
        address = 'http://www.cbr.ru/scripts/XML_daily.asp?date_req='+date_x
        data = urllib.request.urlopen(address)
        stuff = ET.parse(data)
        all = stuff.findall('.//Valute')
        print ('База пустая -  надо добавить всего валют:', len(all))
        for entry in all:
            sid = entry.get('ID')
            charcode=entry.find('CharCode').text
            numcode=entry.find('NumCode').text
            name=entry.find('Name').text
            nominal=entry.find('Nominal').text
            value=entry.find('Value').text.replace(',','.')
            updated_at=datetime.datetime.now()
            cur.execute('''INSERT INTO cbratedaily (date, sid, numcode, charcode, name, nominal, value, updated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s );''', (date, sid, numcode, charcode, name, nominal,value, updated_at) )
        conn.commit()
        cur.close()
    elif time.strftime('%Y-%m-%d') in dates_n:
        print ('Такая дата уже есть:',time.strftime('%Y-%m-%d'))
    else:
        address = 'http://www.cbr.ru/scripts/XML_daily.asp?date_req='+date_x
        data = urllib.request.urlopen(address)
        stuff = ET.parse(data)
        all = stuff.findall('.//Valute')
        for entry in all:
            sid = entry.get('ID')
            charcode=entry.find('CharCode').text
            numcode=entry.find('NumCode').text
            name=entry.find('Name').text
            nominal=entry.find('Nominal').text
            value=entry.find('Value').text.replace(',','.')
            updated_at=datetime.datetime.now()
            cur.execute('''INSERT INTO cbratedaily (date, sid, numcode, charcode, name, nominal, value, updated_at)
                    VALUES ( %s,%s,%s,%s,%s,%s,%s, %s );''', (date, sid, numcode, charcode, name, nominal,value, updated_at) )
        conn.commit()
        cur.close()

# DAG definition
with DAG('cb_daily',
    description='DAG made by SON to get daily rates of CBR',
    default_args=default_args,
    tags=['cbr', 'finmarket'],
    schedule_interval="45 */12 * * *") as dag:
    create_table_cbratedaily = PythonOperator(
        task_id='create_table_cbratedaily',
        python_callable=create_table_cbratedaily,
        dag=dag
    )
    load_data_cbratedaily = PythonOperator(
        task_id='load_data_cbratedaily',
        python_callable=load_data_cbratedaily,
        dag=dag
    )

# TASK pipeline
create_table_cbratedaily >> load_data_cbratedaily
