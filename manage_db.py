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
# si no ponemos esto salta error: ModuleNotFoundError: No module named 'cx_Oracle'
# pero cs_oracle ya no se usa, hay que usar oracledb
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb 

def get_credentials():
    # coge credenciales para hacer conexion con oracle
    # formato: user;passw;db
    ruta_login_txt = r'ruta\f.txt' 

    file = open(ruta_login_txt, 'r') # abrimos el fichero txt

    content = file.read() # leemos el contenido

    lst_login = content.split(';') # separamos los elementos

    user, passw, db = lst_login[0], lst_login[1], lst_login[2]
    
    return user, passw , db


def read_query(path_query): # lee la query de un codigo .sql en la ruta introducida
    # abrimos la query, la leemos y cerramos el fichero
    sql_file = open(path_query, 'r')
    sql_query = sql_file.read()
    sql_file.close()   
    return sql_query

        
def process_query(query):
    # procesa las sentencias de la query para que no haya ; y se puedan ejecutar una a una
    # devuelve una lista de sentencias
    # separamos las sentencias
    lst_sql_sentences = query.split(';')
    for i in range(len(lst_sql_sentences)):
        # quitamos ; para que pueda ejecutarse => errores si hay ; al final, interfiere con python
        sentencia = lst_sql_sentences[i].replace(';', '')
        sentencia = sentencia.split('*/')[-1] # quitamos los comentarios
        sentencia = sentencia.replace('\n', ' ') # quitamos saltos de linea
        sentencia = sentencia.rstrip().lstrip() # quitamos espacios al principio y final
        lst_sql_sentences[i] = sentencia
        
    for i in range(len(lst_sql_sentences)): # por si coge mal la sentencia
        if lst_sql_sentences[i].replace(' ', '') in ['\n', '']:
            lst_sql_sentences.pop(i)
            
    return lst_sql_sentences
            


def sql_get_table(query): # ejecuta una sentencia sql que devuelve una tabla
    oracledb.init_oracle_client() # inicia el cliente

    user, passw, db = get_credentials()
    # credenciales
    un = user
    pw = passw
    cs = db

    # creamos conexion
    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        start = dt.datetime.today() # inicio de ejecucion
        df = pd.read_sql(sql=query, con=connection) # descargamos la db a pd.DataFrame()
        print('\nEXEC TIME:', dt.datetime.today() - start) # mostramos tiempo de descarga
        print('\nTamaño del df: {:,.0f}'.format(len(df)).replace(',', '.'))
    return df


def sql_exec_sentence(query): # ejecuta una sentencia sql cualquiera y no almacena el resultado
    oracledb.init_oracle_client() # inicia el cliente
    
    user, passw, db = get_credentials()
    # credenciales
    un = user
    pw = passw
    cs = db
    # creamos conexion
    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
    

def execute_complete_process(lst_sql_sentences):
    # le pasamos una lista procesada de la funcion: process_query
    df = pd.DataFrame()
    for sentence in lst_sql_sentences:
        if sentence.split(' ')[0].upper() == 'SELECT': # devuelve tabla
            df = sql_get_table(sentence)
        else: # CREATE, DROP, ....
            sql_exec_sentence(sentence)
    return df


def df_to_sql(df, name):
    print('\nCreando la tabla')
    start = dt.datetime.now()
    user, passw, db = get_credentials()
    # credenciales
    un = user
    pw = passw
    cs = db
    
    # formato uri: "oracle://user:password@host"
    uri = "oracle://" + un + ':' + pw + '@' + cs
    
    engine = sqla.create_engine(uri)
    
    # por defecto pandas usa TEXT type para object/string => SQL lo mapea como CLOB o TEXT
    # para que lo lea como CHAR hacemos lo siguiente:
    cols = df.dtypes[df.dtypes=='object'].index # columnas de df que son string
    
    measurer = np.vectorize(len) # medidor de longitud de string vectorizado
    # miramos la longitud maxima de las columnas que son string
    dic_str_max_len = {}
    for col in cols:
        res = measurer(df[col].values.astype(str)).max(axis=0)
        dic_str_max_len[col] = res
    # indicamos que queremos que sea formato sql string no texto, con tamaño maximo calculado
    type_mapping = {col : sqla.types.String(dic_str_max_len[col]) for col in cols}
    
    # miramos si la tabla ya esta creada, en ese caso hacemos DROP antes de cargar la nueva
    # lista de todas las tablas disponibles para el usuario
    tablas = sql_get_table("SELECT table_name FROM user_tables")['TABLE_NAME'].values
    
    if name in tablas: # si la tabla ya esta cargada da error y to_sql no lo detecta por permisos
        sql_exec_sentence("DROP TABLE " + name)
        print('\nTabla anterior eliminada')
    # creamos la tabla
    df.to_sql(name, engine, if_exists='replace', dtype=type_mapping, index=False)
    print('\nEXEC time:', dt.datetime.now() - start)