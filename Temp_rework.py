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
            'Startdato': ['2021-09-17','2021-09-20','2021-09-17','2021-09-10']
            ,'Slutdato': ['2021-09-21','2021-09-22','2021-09-21','2021-09-16']
            ,'Silo': ['401','401','511','512'] 
            ,'Ordrenummer': ['O1','O2','O3','04']
            }
df_temp_silos = pd.DataFrame.from_dict(dict_temp_silos)

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


#**********************************************************************************************************************************************************


# =============================================================================
# New functions - to be implemented in main script
# =============================================================================
def get_rework_silos():
    pass # Query that returns dataframe with requested orders, silos and dates for relevant orders.
    # Return None if no orders relevant.


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
        










if len(df_temp_silos) > 0:
    for i in df_temp_silos.index:
        startdato = df_temp_silos['Startdato'][i]
        slutdato = df_temp_silos['Slutdato'][i]
        silo = df_temp_silos['Silo'][i]
        ordrenummer = df_temp_silos['Ordrenummer'][i]
        # print(df_temp_silos['Startdato'][i], i)
        
        print(get_rework_prøvesmagning(startdato, slutdato, silo, ordrenummer))
        print('*' *50)
        print(get_rework_pakkeri(startdato, slutdato, silo, ordrenummer))
        print('*' *50)
        print(get_rework_komprimatorrum(startdato, slutdato, silo, ordrenummer))

    
        
    
#    Kør hver funktion og gem dem i en samlet dataframe









