import pandas as pd
import os
#import dotenv
import redshift_connector
import requests
from bs4 import BeautifulSoup
import numpy as np
import awswrangler as wr

# cargamos las variables de entorno
#dotenv.load_dotenv()
# leemos las varaibles de entorno
#usuario = os.getenv('User')
#passwd = os.getenv('Password')
#host = os.getenv('host')
#database = os.getenv('db')
#my_schema = os.getenv('my_schema')
#print(my_schema)

#path original.. home en Docker
dag_path = os.getcwd()     
with open(dag_path+'/keys/'+"user.txt",'r') as u:
    usuario= u.read()
with open(dag_path+'/keys/'+"pass.txt",'r') as p:
    passwd= p.read()
with open(dag_path+'/keys/'+"host.txt",'r') as h:
    host= h.read()
with open(dag_path+'/keys/'+"db.txt",'r') as d:
    database= d.read()
with open(dag_path+'/keys/'+"my_schema.txt",'r') as s:
    my_schema= s.read()



def connection_open():
    conn = redshift_connector.connect(
        host=host,
        database=database,
        port=5439,
        user=usuario,
        password=passwd
    )
    # conn.rollback() Regresa al inicio del programa, antes de la ejecución de estos cambios.
    # De esta manera, no se deja ninguna operación a medias.
    conn.rollback()
    conn.autocommit = True
    return conn


def create_table():
    conn = connection_open()
    cursor = conn.cursor()
    cursor.execute(
        f'CREATE TABLE if not exists {my_schema}.faseDeGruposMundial(Selección VARCHAR(30),'
        f' Pts INT, PJ INT, PG INT, PE INT, PP INT, GF INT, GC INT, Dif VARCHAR(5),copa_del_mundo INT,'
        f'Posición_Fifa float, Continente VARCHAR(30), Confederación VARCHAR(30), Total_puntos_fifa float) '
        f'DISTKEY(Selección) SORTKEY(Selección, Posición_Fifa,copa_del_mundo)')
    cursor.close()


def extract_data_worldcup():
    url = 'https://es.wikipedia.org/wiki/Copa_Mundial_de_F%C3%BAtbol_de_2022'
    # en la tabla ya quedo cargado el 2018
    # url='https://es.wikipedia.org/wiki/Copa_Mundial_de_F%C3%BAtbol_de_2018'

    # Lee los datos de la página para obtener los resultados de la fase de grupos
    # #filtranndo por aquellos df con columna dif
    dfs = pd.read_html(url, match='Dif')

    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    titulo = None  # Inicializar la variable 'titulo'
    for title in soup.find_all('title'):
        titulo = title.get_text()
    titulo = titulo.replace(' - Wikipedia, la enciclopedia libre', '')
    titulo = int(titulo.replace('Copa Mundial de Fútbol de ', ''))
    print(titulo)

    dffg = pd.DataFrame()

    for df in dfs:
        try:
            if (df.columns == ['Selección', 'Pts', 'PJ', 'PG', 'PE', 'PP', 'GF', 'GC', 'Dif']).all():
                for i in range(len(df.index)):
                    list_item = []
                    for j in range(9):
                        if type(list(df.iloc[i])[j]) is np.int64:
                            list_item.append(list(df.iloc[i])[j].item())
                        elif type(list(df.iloc[i])[j]) is np.float64:
                            list_item.append(list(df.iloc[i])[j].item())
                        else:
                            list_item.append(list(df.iloc[i])[j])
                    dffg = pd.concat([dffg, pd.DataFrame([list_item])], ignore_index=True)

        except ValueError:
            pass

    dffg = dffg.assign(copa_del_mundo=titulo)

    dffg.rename(columns={dffg.columns[0]: 'Selección',
                         dffg.columns[1]: 'Pts',
                         dffg.columns[2]: 'PJ',
                         dffg.columns[3]: 'PG',
                         dffg.columns[4]: 'PE',
                         dffg.columns[5]: 'PP',
                         dffg.columns[6]: 'GF',
                         dffg.columns[7]: 'GC',
                         dffg.columns[8]: 'Dif',
                         dffg.columns[9]: 'copa_del_mundo'}, inplace=True)
    return dffg


def extract_data_ranking():
    url2 = 'https://es.wikipedia.org/wiki/Anexo:Estad%C3%ADsticas_de_la_clasificaci%C3%B3n_mundial_de_la_FIFA'
    fifadfs = pd.read_html(url2, na_values='$Null$')
    fifadf = pd.DataFrame()

    for fifadf in fifadfs:
        try:
            if (fifadf.columns == ['Pos.', 'V', 'Selección', 'Continente', 'Confederación', 'Total puntos']).all():
                print(fifadf)
                break
        except ValueError:
            pass

    return fifadf


def transform_data():
    # la idea es ver como les fue a los top 50 (actuales en el último mundial)
    # merge entre las dos tablas, y se eliminan los que son nulos, ·
    # es decir quienes no estaban en el top 50 y lo ordeno por posición
    dffg = extract_data_worldcup()
    fifadf = extract_data_ranking()
    dffg_fifa_t = pd.merge(dffg, fifadf, on='Selección', how='left')
    dffg_fifa = dffg_fifa_t[
        ['Selección', 'Pts', 'PJ', 'PG', 'PE', 'PP', 'GF', 'GC', 'Dif', 'copa_del_mundo', 'Pos.', 'Continente',
         'Confederación', 'Total puntos']].dropna().sort_values(by=['Pos.'], ignore_index=True)
    return dffg_fifa


def check_duplicates():
    # chequedo duplicados
    dffg_fifa = transform_data()
    cheqdup = dffg_fifa.drop_duplicates()
    if len(cheqdup) == len(dffg_fifa):
        print("Sin duplicados")
    else:
        print("Es necesario remover duplicados")


def load_data():
    conn = connection_open()
    cursor = conn.cursor()
    dffg_fifa = transform_data()
    # TRAE LO QUE HAY EN LA TABLA
    # sql_read = "SELECT * FROM federicobergada_coderhouse.faseDeGruposMundial"
    # datos_existentes = wr.redshift.read_sql_query(sql_read, con=conn)
    # datos_existentes
    # #### aca se remplaza el metodo insert into (NO SOPORTA ON CONFLICT () DO NOTHING/UPDATE) por el wr.redshift.to_sql
    dffg_fifa_item = pd.DataFrame()

    for i in range(len(dffg_fifa.index)):
        list_item = []
        for j in range(14):
            if type(list(dffg_fifa.iloc[i])[j]) is np.int64:
                list_item.append(list(dffg_fifa.iloc[i])[j].item())
            elif type(list(dffg_fifa.iloc[i])[j]) is np.float64:
                list_item.append(list(dffg_fifa.iloc[i])[j].item())
            else:
                list_item.append(list(dffg_fifa.iloc[i])[j])
        dffg_fifa_item = pd.concat([dffg_fifa_item, pd.DataFrame([list_item])], ignore_index=True)

    # dffg_fifa_item.dtypes

    dffg_fifa_item.rename(columns={dffg_fifa_item.columns[0]: 'selección',
                                   dffg_fifa_item.columns[1]: 'pts',
                                   dffg_fifa_item.columns[2]: 'pj',
                                   dffg_fifa_item.columns[3]: 'pg',
                                   dffg_fifa_item.columns[4]: 'pe',
                                   dffg_fifa_item.columns[5]: 'pp',
                                   dffg_fifa_item.columns[6]: 'gf',
                                   dffg_fifa_item.columns[7]: 'gc',
                                   dffg_fifa_item.columns[8]: 'dif',
                                   dffg_fifa_item.columns[9]: 'copa_del_mundo',
                                   dffg_fifa_item.columns[10]: 'posición_fifa',
                                   dffg_fifa_item.columns[11]: 'continente',
                                   dffg_fifa_item.columns[12]: 'confederación',
                                   dffg_fifa_item.columns[13]: 'total_puntos_fifa'}, inplace=True)

    print(dffg_fifa_item)

    wr.redshift.to_sql(df=dffg_fifa_item, con=conn, table='fasedegruposmundial', schema='federicobergada_coderhouse',
                       mode='upsert', primary_keys=['selección', 'copa_del_mundo'], use_column_names=True)

    cursor.close()
