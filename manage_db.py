# -*- coding: utf-8 -*-
"""
Created on Wed May  3 10:00:46 2023

@author: rmar
"""
import datetime as dt
import pandas as pd
import numpy as np
import oracledb
import sqlalchemy as sqla
import sys
# we need to put following line so the error: ModuleNotFoundError: No module named 'cx_Oracle'
# does not appear, but we use oracledb
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb 

def get_credentials():
    # get credentials from file to make oracle connection
    # format: user;passw;db
    ruta_login_txt = r'ruta\f.txt' 

    file = open(ruta_login_txt, 'r') # open txt file

    content = file.read() # read content

    lst_login = content.split(';') # split elements

    user, passw, db = lst_login[0], lst_login[1], lst_login[2]
    
    return user, passw , db


def read_query(path_query): # read full code in a .sql file in path_query
    # open c.sql, read it and close it
    # return string with the full code
    sql_file = open(path_query, 'r')
    sql_query = sql_file.read()
    sql_file.close()   
    return sql_query

        
def process_query(query):
    # process sentences so ; does not appear (python conflict)
    # we do this to execute the sentences one by one
    # return list of sentences

    # split sentences
    lst_sql_sentences = query.split(';')
    for i in range(len(lst_sql_sentences)):
        sentencia = lst_sql_sentences[i].replace(';', '') # drop ; 
        sentencia = sentencia.split('*/')[-1] # drop comments
        sentencia = sentencia.replace('\n', ' ') # drop line jump
        sentencia = sentencia.rstrip().lstrip() # drop blank spaces 
        lst_sql_sentences[i] = sentencia
        
    for i in range(len(lst_sql_sentences)): # double check
        if lst_sql_sentences[i].replace(' ', '') in ['\n', '']:
            lst_sql_sentences.pop(i)
            
    return lst_sql_sentences
            


def sql_get_table(query):
    # execute one sentence, return pd.DataFrame
    oracledb.init_oracle_client() # start client

    user, passw, db = get_credentials()
    # credentials
    un = user
    pw = passw
    cs = db

    # create connection
    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        start = dt.datetime.today() # exec start
        df = pd.read_sql(sql=query, con=connection) # download db to pd.DataFrame()
        print('\nEXEC TIME:', dt.datetime.today() - start)
        print('\nTamaño del df: {:,.0f}'.format(len(df)).replace(',', '.'))
    return df


def sql_exec_sentence(query):
    # execute one sentence and do not save return
    oracledb.init_oracle_client() # start client
    
    user, passw, db = get_credentials()
    # credentials
    un = user
    pw = passw
    cs = db
    # create conection
    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
    

def execute_complete_process(lst_sql_sentences):
    # lst_sql_sentences is the return from process_query()
    df = pd.DataFrame()
    for sentence in lst_sql_sentences:
        if sentence.split(' ')[0].upper() == 'SELECT': # return TABLE
            df = sql_get_table(sentence)
        else: # CREATE, DROP, ....
            sql_exec_sentence(sentence)
    return df


def df_to_sql(df, name):
    print('\nCreando la tabla')
    start = dt.datetime.now()
    user, passw, db = get_credentials()
    # credentials
    un = user
    pw = passw
    cs = db
    
    # uri format: "oracle://user:password@host"
    uri = "oracle://" + un + ':' + pw + '@' + cs
    
    engine = sqla.create_engine(uri)

    # by default pandas use TEXT type for object/string => SQL maps text into CLOB or TEXT
    # we want SQL to map as CHAR
    cols = df.dtypes[df.dtypes=='object'].index # df string columns
    
    measurer = np.vectorize(len) # length vectorize measurer
    # max length in the string columns
    dic_str_max_len = {}
    for col in cols:
        res = measurer(df[col].values.astype(str)).max(axis=0)
        dic_str_max_len[col] = res
    # stablish the string format
    type_mapping = {col : sqla.types.String(dic_str_max_len[col]) for col in cols}

    # search if table is created, in that case do DROP before CREATE
    # list all table from user
    tablas = sql_get_table("SELECT table_name FROM user_tables")['TABLE_NAME'].values
    
    if name in tablas:
        sql_exec_sentence("DROP TABLE " + name)
        print('\nTabla anterior eliminada')
    # CREATE TABLE
    df.to_sql(name, engine, if_exists='replace', dtype=type_mapping, index=False)
    print('\nEXEC time:', dt.datetime.now() - start)
