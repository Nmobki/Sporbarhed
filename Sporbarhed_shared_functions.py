#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import Sporbarhed_shared_server_information as sssi


# =============================================================================
# Variables for query connections
# =============================================================================
con_ds = sssi.con_ds
engine_ds = sssi.engine_ds
con_nav = sssi.con_nav
con_comscale = sssi.con_comscale
con_probat = sssi.con_probat

# =============================================================================
# Functions
# =============================================================================

# Get connection information from sssi
def get_connection(connection):
    dictionary = {
        'navision': sssi.con_nav
        ,'comscale': sssi.con_comscale
        ,'probat': sssi.con_probat
        ,'bki_datastore': sssi.con_ds }
    return dictionary[connection]

# Get cursor
def get_cursor(connection):
    dictionary = {
        'bki_datastore': sssi.cursor_ds }
    return dictionary[connection]

# Get filepath
def get_filepath(type):
    dictionary = {
        'report': sssi.report_filepath}
    return dictionary[type]

# Get engines for connections
def get_engine(connection):
    dictionary = {
        'bki_datastore': sssi.engine_ds }
    return dictionary[connection]

# Read section names
def get_ds_reporttype(request_type):
    query =  f"""SELECT SRS.[Sektion], SS.[Beskrivelse] AS [Sektion navn]
                       FROM [trc].[Sporbarhed_rapport_sektion] AS SRS
					   INNER JOIN [trc].[Sporbarhed_sektion] AS SS
					   ON SRS.[Sektion] = SS.[Id]
                       WHERE [Forespørgselstype] = {request_type} """
    return pd.read_sql(query, con_ds)

# Get section name for section from query
def get_section_name(section, dataframe):
    df_temp_sections = dataframe.loc[dataframe['Sektion'] == section]
    x = df_temp_sections['Sektion navn'].iloc[0]
    if len(x) == 0 or len(x) > 31:
        return 'Sektion ' + str(section)
    else:
        return x

# Find statuscode for section log
def get_section_status_code(dataframe):
    if len(dataframe) == 0:
        return 1 # Empty dataframe
    else:
        return 99 # Continue

# Write into section log
def section_log_insert(request_id, section, statuscode, errorcode=None):
    df = pd.DataFrame(data={'Forespørgsels_id':request_id,
                            'Sektion':section,
                            'Statuskode':statuscode,
                            'Fejlkode_script':str(errorcode)}
                      , index=[0])
    df.to_sql('Sporbarhed_sektion_log', con=engine_ds, schema='trc', if_exists='append', index=False)

# Write dataframe into Excel sheet
def insert_dataframe_into_excel (engine, dataframe, sheetname, include_index):
    dataframe.to_excel(engine, sheet_name=sheetname, index=include_index)

# Convert list into string for SQL IN operator
def string_to_sql(list_with_values):
    if len(list_with_values) == 0:
        return ''
    else:
        return "'{}'".format("','".join(list_with_values))

def number_format(value, number_type):
    try:
        if number_type == 'dec_2':
            return f'{round(value,2):,}'.replace(',', ';').replace('.', ',').replace(';', '.')
        elif number_type == 'dec_1':
            return f'{round(value,1):,}'.replace(',', ';').replace('.', ',').replace(';', '.')
        elif number_type == 'dec_0':
            return f'{int(round(value,0)):,}'.replace(',', ';').replace('.', ',').replace(';', '.')
        elif number_type == 'pct_2':
            return f'{round(value,4):.2%}'.replace(',', ';').replace('.', ',').replace(';', '.')
        elif number_type == 'pct_0':
            return f'{round(value,2):.0%}'.replace(',', ';').replace('.', ',').replace(';', '.')
        else:
            return value
    except:
        return value

# Prevent division by zero error
def zero_division(nominator, denominator, zero_return):
    dictionary = {'None':None,'Zero':0}
    if denominator in [0,None]:
        return dictionary[zero_return]
    else:
        return nominator / denominator

# Convert placeholder values from dataframe to empty string for Word document
def convert_placeholders_word(string):
    if string in ['None','nan','NaT']:
        return ''
    else:
        return string

# Strip comma from commaseparated strings
def strip_comma_from_string(text):
    text = text.rstrip(',')
    return text.lstrip(',')

# Write into dbo.log
def log_insert(event, note):
    dict_log = {'Note': note
                ,'Event': event}
    pd.DataFrame(data=dict_log, index=[0]).to_sql('Log', con=engine_ds, schema='dbo', if_exists='append', index=False)

# Get info from item table in Navision
# Query for Navision items, used for adding information to item numbers not queried directly from Navision
query_nav_items = """ SELECT [No_] AS [Nummer],[Description] AS [Beskrivelse]
                  ,[Item Category Code] AS [Varekategorikode]
				  ,CASE WHEN [Display Item] = 1 THEN 'Display'
				  WHEN [Item Category Code] = 'FÆR KAFFE' THEN 'Færdigkaffe'
				  WHEN [No_] LIKE '1040%' THEN 'Ristet kaffe'
				  WHEN [No_] LIKE '1050%' THEN 'Formalet kaffe'
				  WHEN [No_] LIKE '1020%' THEN 'Råkaffe'
				  ELSE [Item Category Code] END AS [Varetype]
                  FROM [dbo].[BKI foods a_s$Item] """
df_nav_items = pd.read_sql(query_nav_items, con_nav)

def get_nav_item_info(item_no, field):
    if item_no in df_nav_items['Nummer'].tolist():
        df_temp = df_nav_items[df_nav_items['Nummer'] == item_no]
        return df_temp[field].iloc[0]
    else:
        return None


# Get info from assembly and production orders in Navision
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

def get_nav_order_info(order_no):
    if order_no in df_nav_order_info['Ordrenummer'].tolist():
        df_temp = df_nav_order_info[df_nav_order_info['Ordrenummer'] == order_no]
        return df_temp['Varenummer'].iloc[0]
    else:
        return None

# Get subject for emails depending on request type
def get_email_subject(request_reference, request_type):
    dict_email_subject = {
        0: f'Anmodet rapport for ordre {request_reference}'
        ,1: f'Anmodet rapport for parti {request_reference}'
        ,2: 'Anmodet rapport for opspræt'
        ,3: f'Anmodet rapport for handelsvare {request_reference}'
    }
    return str(dict_email_subject[request_type])


class rework():
    # Get last empty signal from a given silo before requested date
    def get_silo_last_empty(silo, date):
        query = f""" SELECT	MAX(DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)) AS [Dato]
                     FROM [dbo].[PRO_EXP_SILO_DIF]
                     WHERE [SILO] = '{silo}'
                     DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) < '{date}' """
        df = pd.read_sql(query, con_probat)
        if len(df) == 0:
            return None
        else:
            df['Dato'].strftime('%Y-%m-%d')
            return str(df['Dato'].iloc[0])
    
    # Get the first empty signal from a given silo after the requested date
    def get_silo_next_empty(silo, date):
        query = f""" SELECT	MIN(DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)) AS [Dato]
                     FROM [dbo].[PRO_EXP_SILO_DIF]
                     WHERE [SILO] = '{silo}'
                     DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) > '{date}' """
        df = pd.read_sql(query, con_probat)
        if len(df) == 0:
            return None
        else:
            df['Dato'].strftime('%Y-%m-%d')
            return str(df['Dato'].iloc[0])
    
    # Get a dataframe containing all orders which have used rework silos as well as use dates
    def get_rework_silos(orders_string):
        query = f""" SELECT DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Slutdato]
                    ,[SOURCE_NAME] AS [Silo] ,[ORDER_NAME] AS [Produktionsordre]
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
        df = pd.read_sql(query, con_probat)
        df['Startdato'] = df['Silo'].apply((lambda x: rework.get_silo_last_empty(x, df['Slutdato'].strftime('%Y-%m-%d'))))
        return df
    
    # Get rework registrered in prøvesmagning in BKI_Datastore
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
            df_temp = pd.read_sql(query, con_ds)
            if len(df_temp) == 0:
                return None
            else:
                df_temp['Silo'] = silo
                df_temp['Produktionsordre'] = order_no
                df_temp['Kilde'] = 'Prøvesmagning'
                return df_temp
    
    # Fetch start dates from BKI_Datastore and use these to return a list of relevant orders from Navision containing order numbers.
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
            df_ds = pd.read_sql(query_ds, con_ds)
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
                df_total['Produktionsordre'] = order_no
                df_total['Kilde'] = 'Pakkeri'
                return df_total
    
    # Get order numbers registrered in komprimatorrum in BKI_Datastore
    def get_rework_komprimatorrum(start_date, end_date, silo, order_no):
        if None in (start_date, end_date, silo, order_no):
            return None
        else:
            query_ds = f""" SELECT [Referencenummer] AS [Indhold]
                       FROM [BKI_Datastore].[cof].[Rework_tilgang]
                       WHERE Kilde = 2 AND [Silo] = '{silo}'
                       AND DATEADD(D, DATEDIFF(D, 0, [Registreringstidspunkt] ), 0) BETWEEN '{start_date}' AND '{end_date}'
                       GROUP BY [Referencenummer] """
            df_ds = pd.read_sql(query_ds, con_ds)
            if len(df_ds) == 0:
                return None
            else:
                df_ds['Silo'] = silo
                df_ds['Produktionsordre'] = order_no
                df_ds['Kilde'] = 'Komprimatorrum'
                return df_ds
    
    # Fetch start dates from BKI_Datastore and use these to return a list of relevant orders from Navision containing order numbers.
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
            df_ds = pd.read_sql(query_ds, con_ds)
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
                df_total['Produktionsordre'] = order_no
                df_total['Kilde'] = 'Henstandsprøver'
                return df_total

    # Use previously defined functions to create one total dataframe containing all rework identified through various sources
    def get_rework_total(df_silos):
        if len(df_silos) == 0:
            return pd.DataFrame()
        else:
            df_rework = pd.DataFrame()
            for i in df_silos.index:
                startdato = df_silos['Startdato'][i]
                slutdato = df_silos['Slutdato'][i]
                silo = df_silos['Silo'][i]
                ordrenummer = df_silos['Produktionsordre'][i]
                # Functions to get each different type of rework
                df_prøvesmagning = rework.get_rework_prøvesmagning(startdato, slutdato, silo, ordrenummer)
                df_pakkeri = rework.get_rework_pakkeri(startdato, slutdato, silo, ordrenummer)
                df_komprimatorrum = rework.get_rework_komprimatorrum(startdato, slutdato, silo, ordrenummer)
                df_henstandsprøver = rework.get_rework_henstandsprøver(startdato, slutdato, silo, ordrenummer)
                # Concat each function to one dataframe
                df_rework = pd.concat([df_rework, df_prøvesmagning, df_pakkeri, df_komprimatorrum, df_henstandsprøver])
        return df_rework[['Produktionsordre','Silo','Indhold','Kilde']]
