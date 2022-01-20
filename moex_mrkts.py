# Python libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
#import datetime
from datetime import datetime, timedelta
import time
import pendulum
#import mariadb
import pymysql
import sys
sys.path.append('/home/fmsb/fmsb/scripts')
import fmsb
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
#from sqlalchemy.ext.declarative import declarative_base
from urllib.request import urlopen
import json

#variables
secrets = fmsb.secrets()
host=secrets['host']
port=secrets['port']
database=secrets['database']
user=secrets['user']
password=secrets['pass']
engineFMSB = create_engine("mysql+pymysql://"+user+":"+password+"@"+str(host)+":"+str(port)+"/"+database+"?charset=utf8", encoding='utf8')
local_tz=pendulum.timezone("Europe/Moscow")

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021,12,9, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# TASK definition
def _check_table():
    target_table = "ISS_MOEX_MARKETS"
    sql_string_check = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + target_table + "'  and COLUMN_NAME not in ('engine','updated_at');"
    col_names = pd.read_sql_query(sql=sql_string_check, con=engineFMSB)
    if len(col_names)==0:
        return 'sql_create_stm'
    else:
        return 'check_columns'

def sql_create_stm():
    target_table = "ISS_MOEX_MARKETS"
    names=pd.read_sql_query(sql='select name from ISS_MOEX_ENGINES;', con=engineFMSB)
    engines = names['name'].to_list()[0]
    address = 'https://iss.moex.com/iss/engines/'+engines+'/markets.json'
    data = urlopen(address)
    text = json.loads(data.read().decode())
    columnes=text['markets']['columns']
    type=[]
    for col in columnes:
        type.append(text['markets']['metadata'][col]['type'])
    type_adj = list(map(lambda item: item.replace("int32","INTEGER").replace("string", "VARCHAR"), type))
    bytes=[]
    for col in columnes:
        try:
            bytes.append(text['markets']['metadata'][col]['bytes'])
        except:
            bytes.append(None)
    lst_tuple = list(zip(columnes, type_adj, bytes))
    lst_columnes = []
    for line in lst_tuple:
        if line[2] == None:
            lst_columnes.append(target_table + "." + line[0]+" "+line[1])
        else:
            lst_columnes.append(target_table + "." + line[0]+" "+line[1] + "("+str(line[2]) +")")
    col_string=', '.join(lst_columnes)
    sql_string = "CREATE TABLE IF NOT EXISTS " + target_table + "(" + col_string  + ",engine VARCHAR(45),updated_at TIMESTAMP, primary key (" + columnes[0] +"));"
    return(sql_string)

def create_table(ti):
    start_time = time.time()
    target_table = "ISS_MOEX_MARKETS"
    names=pd.read_sql_query(sql='select name from ISS_MOEX_ENGINES;', con=engineFMSB)
    engines = names['name'].to_list()[0]
    address = 'https://iss.moex.com/iss/engines/'+engines+'/markets.json'
    data = urlopen(address)
    text = json.loads(data.read().decode())
    columnes=text['markets']['columns']
    sql_create_stm=ti.xcom_pull(task_ids="sql_create_stm")
    log_string = '''INSERT INTO ISS_MOEX_LOG (table_name, db_name, action, time_to_insert, updated_at) VALUES (%s,%s,%s,%s,%s );'''
    with engineFMSB.connect() as conn:
        conn.execute(sql_create_stm)
        time_to_load = (time.time() - start_time)
        conn.execute(log_string,(target_table, database, "created and inserted: " + str(len(columnes)) + " columns", time_to_load, datetime.now()))

def _check_columns():
    start_time = time.time()
    target_table = "ISS_MOEX_MARKETS"
    sql_string_check = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + target_table + "'  and COLUMN_NAME not in ('engine','updated_at');"
    log_string = '''INSERT INTO ISS_MOEX_LOG (table_name, db_name, action, time_to_insert, updated_at) VALUES (%s,%s,%s,%s,%s );'''
    col_names = pd.read_sql_query(sql=sql_string_check, con=engineFMSB)
    names=pd.read_sql_query(sql='select name from ISS_MOEX_ENGINES;', con=engineFMSB)
    engines = names['name'].to_list()[0]
    address = 'https://iss.moex.com/iss/engines/'+engines+'/markets.json'
    data = urlopen(address)
    text = json.loads(data.read().decode())
    columnes=text['markets']['columns']
    moex_names = pd.DataFrame(columnes,columns = ['MOEX'])
    check_names = pd.merge(col_names, moex_names, left_on="COLUMN_NAME", right_on="MOEX", how="outer")
    check_names['CHECK'] = np.where((check_names['COLUMN_NAME'] == check_names['MOEX']), "ok" , "no")
    check_sum=check_names['CHECK'].nunique()
    if check_sum ==1:
        with engineFMSB.connect() as conn:
            time_to_load = (time.time() - start_time)
            conn.execute(log_string,(target_table, database, "columns are up to date", time_to_load, datetime.now()))
        return 'load_data' #(True)
    else:
        with engineFMSB.connect() as conn:
            time_to_load = (time.time() - start_time)
            conn.execute(log_string,(target_table, database, "should check columns", time_to_load, datetime.now()))
        return 'column_error'  #(False)

def column_error():
    return('should check columnes')

def load_data():
    target_table = "ISS_MOEX_MARKETS"
    start_time = time.time()
    # get columns from table in database
    sql_string_check = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + target_table + "';"
    col_names = pd.read_sql_query(sql=sql_string_check, con=engineFMSB)
    col_names_list = col_names['COLUMN_NAME'].to_list()
    lst_col_insert = []
    for line in col_names_list:
        lst_col_insert.append(target_table + "." + line)
    col_string_insert=', '.join(lst_col_insert)
    val_list = (len(col_names_list))*"%s,"
    #create query
    sql_insert  = "REPLACE INTO "+ target_table+" ("+col_string_insert+") VALUES ("+val_list[:-1]+");"
    #load data
    cnt=0
    names=pd.read_sql_query(sql='select name from ISS_MOEX_ENGINES;', con=engineFMSB)
    engines = names['name'].to_list()
    for engine in engines:
        address = 'https://iss.moex.com/iss/engines/'+engine+'/markets.json'
        data = urlopen(address)
        text = json.loads(data.read().decode())
        loaddata =text['markets']['data']
        vs=[]
        for i in loaddata:
            i.append(engine)
            i.append(datetime.now())
            vs.append(i)
            cnt+=1
        #loading
        with engineFMSB.connect() as conn:
            conn.execute(sql_insert, loaddata)
    time_to_load = (time.time() - start_time)
    # logging
    log_string = '''INSERT INTO ISS_MOEX_LOG (table_name, db_name, action, time_to_insert, updated_at) VALUES (%s,%s,%s,%s,%s );'''
    with engineFMSB.connect() as conn:
        conn.execute(log_string,(target_table, database, "created and inserted: " + str(cnt) + " rows", time_to_load, datetime.now()))
    return(time_to_load)

# DAG definition
with DAG('moex_mrkts', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    check_table = BranchPythonOperator(
    task_id='check_table',
    python_callable=_check_table
    )
    sql_create_stm = PythonOperator(
    task_id='sql_create_stm',
    python_callable=sql_create_stm
    )
    check_columns = BranchPythonOperator(
    task_id='check_columns',
    python_callable=_check_columns
    )
    column_error = PythonOperator(
    task_id='column_error',
    python_callable=column_error
    )
    create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table
    )
    load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    trigger_rule=TriggerRule.ALL_DONE
    )
# TASK pipeline
check_table >> [sql_create_stm, check_columns]
sql_create_stm >> create_table
check_columns >> [load_data, column_error]
[create_table,check_columns] >> load_data
