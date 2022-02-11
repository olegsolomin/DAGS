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
import math

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
def check_table():
    log_string = '''INSERT INTO ISS_MOEX_LOG (table_name, db_name, action, time_to_insert, updated_at) VALUES (%s,%s,%s,%s,%s );'''
    ## getting combo for tables from table with markets and engines
    markets=pd.read_sql_query(sql='select engine,name from ISS_MOEX_MARKETS;', con=engineFMSB)
    em_tuple = [tuple(x) for x in markets[['engine','name']].to_numpy()]
    tables=[]
    for entry in em_tuple:
        ## creating table name
        target_table = entry[0]+'_'+entry[1]+'_trades'
        ## getting info about table
        sql_table_check = "SELECT COUNT(*) COL_NUMBER FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME IN (%(table)s) GROUP BY TABLE_NAME;"
        params = {'table':target_table}
        with engineFMSB.connect() as conn:
            check=conn.execute(sql_table_check, params).fetchall()
        if check == []:
            tables.append(entry)
        if check == []:
            tables.append(entry)
    return(tables)

def create_table(ti):
    start_time = time.time()
    log_string = '''INSERT INTO ISS_MOEX_LOG (table_name, db_name, action, time_to_insert, updated_at) VALUES (%s,%s,%s,%s,%s );'''
    ## getting combo for tables
    em_tuple = ti.xcom_pull(task_ids="check_table")
    for entry in em_tuple:
        ## parsing each combo for columnes, types and size
        target_table = entry[0]+'_'+entry[1]+'_trades'
        address = 'https://iss.moex.com/iss/engines/'+entry[0]+'/markets/'+entry[1]+'/trades.json?start=0'
        data = pd.read_json(address)
    ## reading columns
        columnes=data['trades']['columns']
    ## reading types
        type=[]
        for col in columnes:
            type.append(data['trades']['metadata'][col]['type'])
    ##adjusting types
        type_adj = list(map(lambda item: item.replace("int32","INT").replace("int64","BIGINT").replace("string", "VARCHAR"), type))
        bytes=[]
        for col in columnes:
            try:
                bytes.append(data['trades']['metadata'][col]['bytes'])
            except:
                bytes.append(None)
    ## linking columnes, types and size
        lst_tuple = list(zip(columnes, type_adj, bytes))
        lst_columnes = []
        for line in lst_tuple:
            if line[2] == None:
                lst_columnes.append(target_table + "." + line[0]+" "+line[1])
            elif line[1] in ('date', 'time', 'datetime'):
                lst_columnes.append(target_table + "." + line[0]+" "+line[1])
            else:
                lst_columnes.append(target_table + "." + line[0]+" "+line[1] + "("+str(line[2]) +")")
        col_string=', '.join(lst_columnes)
        create_table = "CREATE TABLE IF NOT EXISTS " + target_table + "(" + col_string  + ",engine VARCHAR(45), market VARCHAR(45),updated_at TIMESTAMP)"
        create_index = "CREATE UNIQUE INDEX TRADES_INDEX ON "+target_table+"(TRADENO, TRADETIME, SECID, BOARDID, SYSTIME) USING BTREE;"
        time_to_load = (time.time() - start_time)
        with engineFMSB.connect() as conn:
            conn.execute(create_table)
            try:
                conn.execute(create_index)
            except:
                continue
            conn.execute(log_string,(target_table, database, "created and inserted: " + str(len(columnes)) + " columns", time_to_load, datetime.now()))

def check_columns():
    start_time = time.time()
    #loop for every table
    ## getting combo for tables from table with markets and engines
    markets=pd.read_sql_query(sql='select engine,name from ISS_MOEX_MARKETS;', con=engineFMSB)
    em_tuple = [tuple(x) for x in markets[['engine','name']].to_numpy()]
    table_to_check=[]
    for entry in em_tuple:
        ## creating table name
        target_table = entry[0]+'_'+entry[1]+'_trades'
        ## getting info about table
        sql_string_check = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + target_table + "'  and COLUMN_NAME not in ('engine', 'market', 'updated_at');"
        log_string = '''INSERT INTO ISS_MOEX_LOG (table_name, db_name, action, time_to_insert, updated_at) VALUES (%s,%s,%s,%s,%s );'''
        col_names = pd.read_sql_query(sql=sql_string_check, con=engineFMSB)
        ##url
        address = 'https://iss.moex.com/iss/engines/'+entry[0]+'/markets/'+entry[1]+'/trades.json?start=0'
        ## reading data from url
        data = pd.read_json(address)
        ## reading columns
        columnes=data['trades']['columns']
        moex_names = pd.DataFrame(columnes,columns = ['MOEX'])
        check_names = pd.merge(col_names, moex_names, left_on="COLUMN_NAME", right_on="MOEX", how="outer")
        check_names['CHECK'] = np.where((check_names['COLUMN_NAME'] == check_names['MOEX']), "ok" , "no")
        check_sum=check_names['CHECK'].nunique()
        #print(check_names,target_table, check_sum)
        if check_sum != 1:
            time_to_load = (time.time() - start_time)
            table_to_check.append(target_table)
    for i in table_to_check:
        with engineFMSB.connect() as conn:
            conn.execute(log_string,(i, database, "! check columns !", time_to_load, datetime.now()))
    return(table_to_check)

def load_data():
    log_string = '''INSERT INTO ISS_MOEX_LOG (table_name, db_name, action, time_to_insert, updated_at) VALUES (%s,%s,%s,%s,%s );'''
    ## loop for every table ## getting combo for tables from table with markets and engines
    markets=pd.read_sql_query(sql='select engine,name from ISS_MOEX_MARKETS;', con=engineFMSB)
    em_tuple = [tuple(x) for x in markets[['engine','name']].to_numpy()]
    for entry in em_tuple:
        start_time = time.time()
        ## creating table name
        target_table = entry[0]+'_'+entry[1]+'_trades'
        #max_tradeno
        with engineFMSB.connect() as conn:
            max_tradeno=conn.execute("select max(tradeno) from "+target_table+";").fetchone()[0]
        if max_tradeno == None:
            max_tradeno = 0
        # for sql_string
        sql_string_col = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '" + target_table + "';"
        col_names = pd.read_sql_query(sql=sql_string_col, con=engineFMSB)
        col_names_list = col_names['COLUMN_NAME'].to_list()
        lst_col_insert = []
        for line in col_names_list:
            lst_col_insert.append(target_table + "." + line)
        col_string_insert=', '.join(lst_col_insert)
        val_list = (len(col_names_list))*"%s,"
        #create query
        sql_insert  = "REPLACE INTO "+ target_table+" ("+col_string_insert+") VALUES ("+val_list[:-1]+");"
        #loop to load data
        step = 5000
        max_page_number = 100000
        for j in range(0,max_page_number):
            load_address = 'https://iss.moex.com/iss/engines/'+entry[0]+'/markets/'+entry[1]+'/trades.json?start='+str(step*j)
            raw_load_data = pd.read_json(load_address, precise_float=True)
            load_data = raw_load_data['trades']['data']
            if load_data !=[]:
                total=0
                load_data_adj =[]
                for d in load_data:
                    if d[0]>max_tradeno:
                        d.append(entry[0])
                        d.append(entry[1])
                        d.append(datetime.now())
                        load_data_adj.append(d)
                        total=+1
                        with engineFMSB.connect() as conn:
                            conn.execute(sql_insert,d)
                    else:
                        pass
            else:
                break
        time_to_load = (time.time() - start_time)

        with engineFMSB.connect() as conn:
            conn.execute(log_string,(target_table, database, "inserted   " + str(total) + "  row", time_to_load, datetime.now()))
    return(time_to_load)

# DAG definition
with DAG('moex_trades',
    schedule_interval='20 */4 * * *',
    default_args=default_args,
    tags=['moex', 'finmarket'],
    catchup=False) as dag:
    check_table = PythonOperator(
    task_id='check_table',
    python_callable=check_table
    )
    check_columns = PythonOperator(
    task_id='check_columns',
    python_callable=check_columns
    )
    create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table
    )
    load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    trigger_rule=TriggerRule.ALL_SUCCESS
    )
# TASK pipeline
check_table >> create_table >> check_columns >> load_data
