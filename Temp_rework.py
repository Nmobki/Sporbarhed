# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import urllib
import Sporbarhed_shared_functions as ssf

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
            'Startdato': ['2021-09-17','2021-09-20','2021-09-17','2021-09-10']
            ,'Slutdato': ['2021-09-21','2021-09-22','2021-09-21','2021-09-16']
            ,'Silo': ['401','401','511','512'] 
            ,'Ordrenummer': ['O1','O2','O3','04']
            }
df_temp_silos = pd.DataFrame.from_dict(dict_temp_silos)

print(df_temp_silos)


# =============================================================================
# New functions - to be implemented in main script
# =============================================================================
def get_silo_last_empty(silo, date):
    query = f""" """
    df = pd.read_sql(query, con_probat)
    
    if len(df) == 0:
        return None
    else:
        return 'string' # Change this..

def get_rework_silos(orders_string):
    query = f""" SELECT DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                ,[SOURCE_NAME] AS [Silo] ,[ORDER_NAME] AS [Ordrenummer]
                FROM [dbo].[PRO_EXP_ORDER_UNLOAD_G]
                WHERE [SOURCE_NAME] IN ('511','512') AND [ORDER_NAME] IN ({orders_string})
                GROUP BY DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) ,[SOURCE_NAME] ,[ORDER_NAME]
                UNION ALL
                SELECT	DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                ,[SOURCE] ,[ORDER_NAME]
                FROM [dbo].[PRO_EXP_ORDER_LOAD_G]
                WHERE [SOURCE] in ('401','403') AND [ORDER_NAME] IN ({orders_string})
                GROUP BY DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                ,[SOURCE] ,[ORDER_NAME] """
    pass # Query that returns dataframe with requested orders, silos and dates for relevant orders.
    # Return empty dataframe if no orders relevant.


def get_rework_prøvesmagning(start_date, end_date, silo, order_no):
    if None in (start_date, end_date, silo, order_no):
        return None
    else:
        query = f""" SELECT	RP.[Produktionsordrenummer] AS [Indhold]
                FROM [cof].[Rework_tilgang] AS RT
                INNER JOIN [cof].[Rework_prøvesmagning] AS RP
                    ON RT.[Referencenummer] = RP.[Referencenummer]
                WHERE RT.[Kilde] = 0
                    AND RT.[Silo] = '{silo}'
                    AND DATEADD(D, DATEDIFF(D, 0, RT.[Registreringstidspunkt] ), 0) BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY RP.[Produktionsordrenummer] """
        df_temp = pd.read_sql(query, con_04)
        if len(df_temp) == 0:
            return None
        else:
            df_temp['Silo'] = silo
            df_temp['Ordrenummer'] = order_no
            df_temp['Kilde'] = 'Prøvesmagning'
            return df_temp

def get_rework_pakkeri(start_date, end_date, silo, order_no):
    if None in (start_date, end_date, silo, order_no):
        return None
    else:
        query_ds = f""" SELECT DATEADD(D, DATEDIFF(D, 0, [Registreringstidspunkt] ), 0) AS [Dato]
                   ,[Silo],[Reworktype]
                   FROM [BKI_Datastore].[cof].[Rework_tilgang]
                   WHERE Kilde = 1 AND [Silo] = '{silo}'
                   AND DATEADD(D, DATEDIFF(D, 0, [Registreringstidspunkt] ), 0) BETWEEN '{start_date}' AND '{end_date}'
                   GROUP BY
                   DATEADD(D, DATEDIFF(D, 0, [Registreringstidspunkt] ), 0)
                   ,[Silo],[Reworktype] """
        df_ds = pd.read_sql(query_ds, con_04)
        df_total = pd.DataFrame()
        if len(df_ds) == 0:
            return None
        else:
            for i in df_ds.index:
                dato = df_ds['Dato'][i].strftime('%Y-%m-%d')
                rework_type = df_ds['Reworktype'][i]
                query_nav = f""" WITH NAV_CTE AS ( SELECT ILE.[Posting Date] AS [Dato]
                        	,ILE.[Document No_] AS [Indhold]
                        	,CASE WHEN I.[Produktionskode] LIKE '%HB' THEN 2
                            	ELSE 1 END AS [Kaffetype]
                            FROM [dbo].[BKI foods a_s$Item Ledger Entry] AS ILE
                            INNER JOIN [dbo].[BKI foods a_s$Item] AS I
                            	ON ILE.[Item No_] = I.[No_]
                            WHERE ILE.[Entry Type] = 6
                            	AND I.[Item Category Code] = 'FÆR KAFFE'
                            GROUP BY ILE.[Posting Date] ,ILE.[Item No_],ILE.[Document No_]
                            	,CASE WHEN I.[Produktionskode] LIKE '%HB' THEN 2
                            	ELSE 1 END )
                            SELECT * FROM NAV_CTE WHERE [Dato] = '{dato}' AND [Kaffetype] = {rework_type} """
                df_nav = pd.read_sql(query_nav, con_nav)
                df_total = pd.concat([df_total, df_nav])
        if len(df_total) == 0:
            return None
        else:
            df_total['Silo'] = silo
            df_total['Ordrenummer'] = order_no
            df_total['Kilde'] = 'Pakkeri'
            return df_total
                
def get_rework_komprimatorrum(start_date, end_date, silo, order_no):
    if None in (start_date, end_date, silo, order_no):
        return None
    else:
        query_ds = f""" SELECT [Referencenummer] AS [Indhold]
                   FROM [BKI_Datastore].[cof].[Rework_tilgang]
                   WHERE Kilde = 2 AND [Silo] = '{silo}'
                   AND DATEADD(D, DATEDIFF(D, 0, [Registreringstidspunkt] ), 0) BETWEEN '{start_date}' AND '{end_date}'
                   GROUP BY [Referencenummer] """
        df_ds = pd.read_sql(query_ds, con_04)
        if len(df_ds) == 0:
            return None
        else:
            df_ds['Silo'] = silo
            df_ds['Ordrenummer'] = order_no
            df_ds['Kilde'] = 'Komprimatorrum'
            return df_ds
        

def get_rework_henstandsprøver(start_date, end_date, silo, order_no):
    if None in (start_date, end_date, silo, order_no):
        return None
    else:
        query_ds = f""" SELECT [Startdato] AS [Dato]
                   ,[Silo],[Reworktype]
                   FROM [BKI_Datastore].[cof].[Rework_tilgang]
                   WHERE Kilde = 3 AND [Silo] = '{silo}'
                   AND DATEADD(D, DATEDIFF(D, 0, [Registreringstidspunkt] ), 0) BETWEEN '{start_date}' AND '{end_date}'
                   GROUP BY [Startdato],[Silo],[Reworktype] """
        df_ds = pd.read_sql(query_ds, con_04)
        df_total = pd.DataFrame()
        if len(df_ds) == 0:
            return None
        else:
            for i in df_ds.index:
                dato = df_ds['Dato'][i].strftime('%Y-%m-%d')
                rework_type = df_ds['Reworktype'][i]
                query_nav = f""" WITH NAV_CTE AS ( SELECT ILE.[Posting Date] AS [Dato]
                        	,ILE.[Document No_] AS [Indhold]
                        	,CASE WHEN I.[Produktionskode] LIKE '%HB' THEN 2
                            	ELSE 1 END AS [Kaffetype]
                            FROM [dbo].[BKI foods a_s$Item Ledger Entry] AS ILE
                            INNER JOIN [dbo].[BKI foods a_s$Item] AS I
                            	ON ILE.[Item No_] = I.[No_]
                            WHERE ILE.[Entry Type] = 6
                            	AND I.[Item Category Code] = 'FÆR KAFFE'
                            GROUP BY ILE.[Posting Date] ,ILE.[Item No_],ILE.[Document No_]
                            	,CASE WHEN I.[Produktionskode] LIKE '%HB' THEN 2
                            	ELSE 1 END )
                            SELECT * FROM NAV_CTE WHERE [Dato] = '{dato}' AND [Kaffetype] = {rework_type} """
                df_nav = pd.read_sql(query_nav, con_nav)
                df_total = pd.concat([df_total, df_nav])
        if len(df_total) == 0:
            return None
        else:
            df_total['Silo'] = silo
            df_total['Ordrenummer'] = order_no
            df_total['Kilde'] = 'Henstandsprøver'
            return df_total


def get_rework_total(df_silos):
    if len(df_silos) == 0:
        return None
    else:
        df_rework = pd.DataFrame()
        for i in df_silos.index:
            startdato = df_silos['Startdato'][i]
            slutdato = df_silos['Slutdato'][i]
            silo = df_silos['Silo'][i]
            ordrenummer = df_silos['Ordrenummer'][i]
            # Functions to get each different type of rework
            df_prøvesmagning = get_rework_prøvesmagning(startdato, slutdato, silo, ordrenummer)
            df_pakkeri = get_rework_pakkeri(startdato, slutdato, silo, ordrenummer)
            df_komprimatorrum = get_rework_komprimatorrum(startdato, slutdato, silo, ordrenummer)
            df_henstandsprøver = get_rework_henstandsprøver(startdato, slutdato, silo, ordrenummer)
            # Concat each function to one dataframe
            df_rework = pd.concat([df_rework, df_prøvesmagning, df_pakkeri, df_komprimatorrum, df_henstandsprøver])
    return df_rework[['Ordrenummer','Silo','Indhold','Kilde']]




print(get_rework_total(df_temp_silos))





