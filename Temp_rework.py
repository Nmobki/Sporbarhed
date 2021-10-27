# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import urllib

# =============================================================================
# Variables for query connections
# =============================================================================
server_04 = 'sqlsrv04'
db_04 = 'BKI_Datastore'
con_04 = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_04};DATABASE={db_04};autocommit=True')
params_04 = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_04};DATABASE={db_04};Trusted_Connection=yes')
engine_04 = create_engine(f'mssql+pyodbc:///?odbc_connect={params_04}')
cursor_04 = con_04.cursor()

server_nav = r'SQLSRV03\NAVISION'
db_nav = 'NAV100-DRIFT'
con_nav = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_nav};DATABASE={db_nav};Trusted_Connection=yes')
params_nav = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_nav};DATABASE={db_nav};Trusted_Connection=yes')
engine_nav = create_engine(f'mssql+pyodbc:///?odbc_connect={params_nav}')

server_probat = '192.168.125.161'
db_probat = 'BKI_IMP_EXP'
con_probat = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_probat};DATABASE={db_probat};uid=bki_read;pwd=Probat2016')
params_probat = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_probat};DATABASE={db_probat};Trusted_Connection=yes')
engine_probat = create_engine(f'mssql+pyodbc:///?odbc_connect={params_probat}')


# =============================================================================
# Temp data instead of actual query
# Actual query needs to find start and end before the actual date
# =============================================================================

dict_temp_silos = {
            'Dato': ['2021-09-19','2021-09-20','2021-09-17','2021-09-16']
            ,'Startdato': ['2021-09-17','2021-09-20','2021-09-17','2021-09-10']
            ,'Slutdato': ['2021-09-20','2021-09-25','2021-09-21','2021-09-16']
            ,'Silo': ['401','401','511','512'] 
            ,'Ordrenummer': ['O1','O2','O3','04']
            }
df_temp_silos = pd.DataFrame.from_dict(dict_temp_silos)
#Slutdato 2 skal implementeres i sidste udgave. Det giver ikke mening at spore på noget, der er tilsæt efter produktion
df_temp_silos['Slutdato2'] = df_temp_silos[['Dato','Slutdato']].min(axis=1)

print(df_temp_silos)

# =============================================================================
# Queries already existing in main script
# =============================================================================
# Query for getting item numbers for production and assembly orders from Navision
query_nav_order_info = """ SELECT PAH.[No_] AS [Ordrenummer]
                       ,PAH.[Item No_] AS [Varenummer]
                       FROM [dbo].[BKI foods a_s$Posted Assembly Header] AS PAH
                       INNER JOIN [dbo].[BKI foods a_s$Item] AS I
                           ON PAH.[Item No_] = I.[No_]
                       WHERE I.[Item Category Code] = 'FÆR KAFFE'
                           AND I.[Display Item] = 1
                       UNION ALL
                       SELECT PO.[No_],PO.[Source No_]
                       FROM [dbo].[BKI foods a_s$Production Order] AS PO
                       INNER JOIN [dbo].[BKI foods a_s$Item] AS I
                           ON PO.[Source No_] = I.[No_]
                       WHERE PO.[Status] IN (2,3,4)
                           AND I.[Item Category Code] <> 'RÅKAFFE' """
df_nav_order_info = pd.read_sql(query_nav_order_info, con_nav)

# =============================================================================
# Functions from main script
# =============================================================================
# Get info from assembly and production orders in Navision
# Function exists already in main script
def get_nav_order_info(order_no):
    if order_no in df_nav_order_info['Ordrenummer'].tolist(): 
        df_temp = df_nav_order_info[df_nav_order_info['Ordrenummer'] == order_no]
        return df_temp['Varenummer'].iloc[0]
    else:
        return None













