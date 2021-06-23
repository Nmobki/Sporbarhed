#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import urllib
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import docx
import openpyxl


# =============================================================================
# Define functions
# =============================================================================

# Get visibility for section from query
def get_section_visibility(dataframe, section):
    return dataframe['Sektion_synlig'].iloc[section]

# Get section name for section from query
def get_section_name(section):
    x = df_sections['Sektion navn'].iloc[section-1]
    if len(x) == 0 or len(x) > 31:
        return 'Sektion ' + str(section)
    else:
        return x

# Find statuscode for section log
def get_section_status_code(dataframe, visibility):
    if visibility == 0:
        return 3 # Not active for selected reporting type
    if len(dataframe) == 0:
        return 1 # Empty dataframe
    else:
        return 99 # Continue

# Write into section log
def section_log_insert(start_time, section, statuscode):
    df = pd.DataFrame(data={'Forespørgsels_id':req_id,'Sektion':section, 'Statuskode':statuscode, 'Start_tid':start_time}, index=[0])
    df.to_sql('Sporbarhed_sektion_log', con=engine_04, schema='trc', if_exists='append', index=False)

# Write dataframe into Excel sheet
def insert_dataframe_into_excel (dataframe, sheetname):
    dataframe.to_excel(excel_writer, sheet_name=sheetname)

# Convert list into string for SQL IN operator
def string_to_sql(list_with_values):
    if len(list_with_values) == 0:
        return ''
    else:
        return "'{}'".format("','".join(list_with_values))


# =============================================================================
# Variables for query connections
# =============================================================================
server_04 = 'sqlsrv04'
db_04 = 'BKI_Datastore'
con_04 = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_04};DATABASE={db_04}')
params_04 = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_04};DATABASE={db_04};Trusted_Connection=yes')
engine_04 = create_engine(f'mssql+pyodbc:///?odbc_connect={params_04}')
cursor_04 = con_04.cursor()

server_nav = 'sqlsrv03\navision'
db_nav = 'NAV100-DRIFT'
# con_nav = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server_nav};DATABASE={db_nav};Trusted_Connection=yes')
# params_nav = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_nav};DATABASE={db_nav};Trusted_Connection=yes')
# engine_nav = create_engine(f'mssql+pyodbc:///?odbc_connect={params_nav}')

server_comscale = r'comscale-bki\sqlexpress'
db_comscale = 'ComScaleDB'
con_comscale = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_comscale};DATABASE={db_comscale}')
params_comscale = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_comscale};DATABASE={db_comscale};Trusted_Connection=yes')
engine_comscale = create_engine(f'mssql+pyodbc:///?odbc_connect={params_comscale}')

server_probat = '192.168.125.161'
db_probat = 'BKI_IMP_EXP'
con_probat = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_probat};DATABASE={db_probat};uid=bki_read;pwd=Probat2016')
params_probat = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_probat};DATABASE={db_probat};Trusted_Connection=yes')
engine_probat = create_engine(f'mssql+pyodbc:///?odbc_connect={params_probat}')

# =============================================================================
# Read data from request
# =============================================================================
query_ds_request =  """ SELECT TOP 1 [Id] ,[Forespørgselstype] ,[Rapporttype]
                    ,[Rapport_modtager] 
                    ,[Referencenummer] AS [Ordrenummer] ,[Note_forespørgsel] 
                    FROM [trc].[Sporbarhed_forespørgsel]
                    WHERE [Forespørgsel_igangsat] IS NULL
                    AND [Referencetype] = 0 AND [Forespørgselstype] = 0 """
df_request = pd.read_sql(query_ds_request, con_04)

# Exit script if no request data is found
if len(df_request) == 0:
    raise SystemExit(0)

# =============================================================================
# Set request variables
# =============================================================================
req_type = df_request.loc[0, 'Forespørgselstype']
req_report_type = df_request.loc[0, 'Rapporttype']
req_order_no = df_request.loc[0, 'Ordrenummer']
req_recipients = df_request.loc[0, 'Rapport_modtager']
req_note = df_request.loc[0, 'Note_forespørgsel']
req_id = df_request.loc[0, 'Id']

# =============================================================================
# Variables for files generated
# =============================================================================
script_name = 'Sporbarhed_færdigkaffe.py'
filepath = r'\\filsrv01\BKI\11. Økonomi\04 - Controlling\NMO\4. Kvalitet\Sporbarhedstest\Tests' # Ændre ifbm. drift

doc = docx.Document()
doc_name = f'Sporbarhedstest_{req_order_no}_{req_id}.docx'
path_file_doc = filepath + r'\\' + doc_name

wb = openpyxl.Workbook()
wb_name = f'Sporbarhedstest_{req_order_no}_{req_id}.xlsx'
path_file_wb = filepath + r'\\' + wb_name
excel_writer = pd.ExcelWriter(path_file_wb, engine='xlsxwriter')

# =============================================================================
# Read setup for section for reporttypes
# =============================================================================
query_ds_reporttypes =  f"""SELECT SRS.[Sektion] ,SRS.[Sektion_synlig] ,SS.[Beskrivelse] AS [Sektion navn]
                       FROM [trc].[Sporbarhed_rapport_sektion] AS SRS
					   INNER JOIN [trc].[Sporbarhed_sektion] AS SS
					   ON SRS.[Sektion] = SS.[Id]
                       WHERE [Rapporttype] = {req_type} 
                       AND [Forespørgselstype] = {req_report_type}"""
df_sections = pd.read_sql(query_ds_reporttypes, con_04)

# =============================================================================
# Queries for different parts of report
# =============================================================================
query_ds_generelt = f""" WITH [KP] AS ( SELECT [Ordrenummer]
                	,SUM( CASE WHEN [Prøvetype] = 0 THEN [Antal_prøver] ELSE 0 END) AS [Kontrolprøve]
                	,SUM( CASE WHEN [Prøvetype] = 1 THEN [Antal_prøver] ELSE 0 END) AS [Referenceprøve]
                	,SUM( CASE WHEN [Prøvetype] = 2 THEN [Antal_prøver] ELSE 0 END) AS [Henstandsprøve]
                    FROM [cof].[Kontrolskema_prøver]
                    GROUP BY [Ordrenummer] )
                    ,[SK] AS ( SELECT [Referencenummer] ,MAX([Status]) AS [Status]
                    FROM [cof].[Smageskema] WHERE [Referencetype] = 2
                    GROUP BY [Referencenummer] )
                    SELECT SF.[Referencenummer] AS [Ordrenummer] ,SF.[Pakketidspunkt] ,KH.[Igangsat_af] AS [Igangsat af]
                    ,KH.[Silo_opstart] AS [Opstartssilo] ,KH.[Taravægt] ,KH.[Nitrogen]
                    ,KH.[Bemærkning] AS [Bemærkning opstart] ,ISNULL(KP.[Kontrolprøve] ,0) AS [Kontrolprøver]
                    ,ISNULL(KP.[Referenceprøve] ,0) AS [Referenceprøver]
                    ,ISNULL(KP.[Henstandsprøve] ,0) AS [Henstandsprøver]
                    ,CASE WHEN SK.[Status] = 1 THEN 'Godkendt' WHEN SK.[Status] = 0 THEN 'Afvist'
                    ELSE 'Ej smagt' END AS [Smagning status], KH.[Pakkelinje]
                    FROM [trc].[Sporbarhed_forespørgsel] AS SF
                    LEFT JOIN [cof].[Kontrolskema_hoved] AS KH ON SF.[Referencenummer] = KH.[Ordrenummer]
                    LEFT JOIN [KP] ON SF.[Referencenummer] = KP.[Ordrenummer]
                    LEFT JOIN [SK] ON SF.[Referencenummer] = SK.[Referencenummer]
                    WHERE SF.[Id] = {req_id} """
df_results_generelt = pd.read_sql(query_ds_generelt, con_04)

query_ds_samples = f""" SELECT KP.[Id],KP.[Ordrenummer],KP.[Registreringstidspunkt]
            	   ,KP.[Registreret_af] AS [Operatør],KP.[Bemærkning]
                   ,KP.[Prøvetype] AS [Prøvetype int],P.[Beskrivelse] AS [Prøvetype]
                   ,CASE WHEN KP.[Kontrol_mærkning] = 1 THEN 'Ok' 
                   WHEN KP.[Kontrol_mærkning] = 0 THEN 'Ej ok' END AS [Mærkning]
                   ,CASE WHEN KP.[Kontrol_rygsvejning] = 1	THEN 'Ok'
                   WHEN KP.[Kontrol_rygsvejning] = 0 THEN 'Ej ok' END AS [Rygsvejsning]
                   ,CASE WHEN KP.[Kontrol_ventil] = 1 THEN 'Ok'
                   WHEN KP.[Kontrol_ventil] = 0 THEN 'Ej ok' END AS [Ventil]
                   ,CASE WHEN KP.[Kontrol_peelbar] = 1	THEN 'Ok'
                   WHEN KP.[Kontrol_peelbar] = 0 THEN 'Ej ok' END AS [Peelbar]
                   ,CASE WHEN KP.[Kontrol_tintie] = 1 THEN 'Ok'
                   WHEN KP.[Kontrol_tintie] = 0 THEN 'Ej ok' END AS [Tintie]
				   ,CASE WHEN KP.[Kontrol_tæthed] = 1 THEN 'Ok'
                   WHEN KP.[Kontrol_tæthed] = 0 THEN 'Ej ok' END AS [Tæthed]
                   ,KP.[Vægt_aflæst] AS [Vægt],KP.[Kontrol_ilt] AS [Ilt],KP.[Silo]
                   ,CASE WHEN SK.[Status] = 1 THEN 'Godkendt' WHEN SK.[Status] = 0
                   THEN 'Afvist' ELSE 'Ej smagt' END AS [Smagning status]
				   ,KP.[Antal_prøver] AS [Antal prøver]
                   FROM [cof].[Kontrolskema_prøver] AS KP
                   INNER JOIN [cof].[Prøvetype] AS P
                        ON KP.[Prøvetype] = P.[Id]
                   LEFT JOIN [cof].[Smageskema] AS SK
                       ON KP.[Id] = SK.[Id_org]
                       AND SK.[Id_org_kildenummer] = 6
                   WHERE KP.[Ordrenummer] = '{req_order_no}' """
df_prøver = pd.read_sql(query_ds_samples, con_04)

query_ds_karakterer = f""" SELECT [Id] ,[Dato] ,[Bruger] ,[Smag_Syre]
                      ,[Smag_Krop] ,[Smag_Aroma] ,[Smag_Eftersmag]
                      ,[Smag_Robusta] ,[Bemærkning]
                      FROM [cof].[Smageskema]
                      WHERE [Referencetype] = 2	
                          AND [Referencenummer] = '{req_order_no}' """
df_karakterer = pd.read_sql(query_ds_karakterer, con_04)

query_ds_section_log = f""" SELECT	SL.[Sektion] AS [Sektionskode]
                       ,S.[Beskrivelse] AS [Sektion],SS.[Beskrivelse] AS [Status]
                       ,SL.[Start_tid],SL.[Registreringstidspunkt] AS [Slut tid]
                	   ,DATEDIFF(ms, SL.[Start_tid] ,SL.[Registreringstidspunkt]) / 1000.0 AS [Sekunder]
                       FROM [trc].[Sporbarhed_sektion_log] AS SL
                       INNER JOIN [trc].[Sporbarhed_sektion] AS S
                         	ON SL.[Sektion] = S.[Id]
                       INNER JOIN [trc].[Sporbarhed_statuskode] AS SS
                            ON SL.[Statuskode] = SS.[Id]
                       WHERE SL.[Forespørgsels_id] = {req_id} """

query_com_statistics = f""" WITH CTE AS ( SELECT SD.[Nominal] ,SD.[Tare]
                            ,SUM( SD.[MeanValueTrade] * SD.[CounterGoodTrade] ) AS [Total vægt]
                            ,SUM( SD.[StandardDeviationTrade] * SD.[CounterGoodTrade] ) AS [Std afv]
                        	,SUM( SD.[CounterGoodTrade] ) AS [Antal enheder]
                        FROM [ComScaleDB].[dbo].[StatisticData] AS SD
                        INNER JOIN [dbo].[Statistic] AS S ON SD.[Statistic_ID] = S.[ID]
                        WHERE S.[Order] = '{req_order_no}' AND lower(S.[ArticleNumber]) NOT LIKE '%k'
                        GROUP BY S.[Order],SD.[Nominal],SD.[Tare] )
                        SELECT CTE.[Total vægt],CTE.[Antal enheder]
                        ,CASE WHEN CTE.[Antal enheder] = 0 
                        THEN NULL ELSE CTE.[Total vægt] / CTE.[Antal enheder] END AS [Middelvægt]
                        ,CASE WHEN CTE.[Antal enheder] = 0 
                        THEN NULL ELSE CTE.[Std afv] / CTE.[Antal enheder] END AS [Standardafvigelse]
                        ,CASE WHEN CTE.[Antal enheder] = 0 
                        THEN NULL ELSE CTE.[Total vægt] / CTE.[Antal enheder] END - CTE.[Nominal] AS [Gns. godvægt per enhed]
                        ,CTE.[Total vægt] - CTE.[Nominal] * CTE.[Antal enheder] AS [Godvægt total]
                        ,CTE.[Nominal] AS [Nominel vægt],CTE.[Tare] AS [Taravægt]
                        FROM CTE """
df_com_statistics = pd.read_sql(query_com_statistics, con_comscale)

# OBS!!! Denne liste skal dannes ud fra NAV forespørgsel når Jira er på plads!!!!
related_orders = string_to_sql(['041367','041344','041234'])

query_probat_ulg = f""" SELECT MIN(DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)) AS [Dato]
                        ,[PRODUCTION_ORDER_ID] AS [Probat id] ,MIN([SOURCE_NAME]) AS [Mølle]
                        ,[ORDER_NAME] AS [Ordrenummer] ,[D_CUSTOMER_CODE] AS [Receptnummer]
                        ,SUM([WEIGHT]) / 1000.0 AS [Kilo]
                        FROM [dbo].[PRO_EXP_ORDER_UNLOAD_G]
                        WHERE [ORDER_NAME] IN ({related_orders})
                        GROUP BY [PRODUCTION_ORDER_ID],[ORDER_NAME]
                    	,[D_CUSTOMER_CODE] """
df_probat_ulg = pd.read_sql(query_probat_ulg, con_probat)


query_probat_lg = f""" SELECT [S_ORDER_NAME]
                       FROM [dbo].[PRO_EXP_ORDER_LOAD_G]
                       WHERE [ORDER_NAME] IN ({related_orders})
                       GROUP BY	[S_ORDER_NAME] """
if len(df_probat_ulg) != 0: # Add to list only if dataframe is not empty
    df_probat_lg = pd.read_sql(query_probat_lg, con_probat)
    related_orders = related_orders + ',' + string_to_sql(df_probat_lg['S_ORDER_NAME'].unique().tolist())


query_probat_ulr = f""" SELECT [S_CUSTOMER_CODE] AS [Recept]
                        ,MIN(DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)) AS [Dato]
                        ,[SOURCE_NAME] AS [Rister] ,[PRODUCTION_ORDER_ID] AS [Probat id]
                    	,[ORDER_NAME] AS [Ordrenummer] ,SUM([WEIGHT]) / 1000.0 AS [Kilo]
                        FROM [dbo].[PRO_EXP_ORDER_UNLOAD_R]
                        WHERE [ORDER_NAME] IN ({related_orders})
                        GROUP BY [S_CUSTOMER_CODE],[SOURCE_NAME],[PRODUCTION_ORDER_ID]
                        ,[ORDER_NAME] """
df_probat_ulr = pd.read_sql(query_probat_ulr, con_probat)


# =============================================================================
# Section 1: Generelt
# =============================================================================
section_id = 1
section_name = get_section_name(section_id)
timestamp = datetime.now()
column_order = ['Varenummer', 'Varenavn', 'Basisenhed', 'Receptnummer', 'Pakkelinje',
                'Produktionsdato', 'Pakketidspunkt', 'Stregkode', 'Ordrenummer',
                'Smagning status', 'Opstartssilo', 'Igangsat af', 'Taravægt',
                'Nitrogen', 'Henstandsprøver', 'Referenceprøver', 'Kontrolprøver',
                'Bemærkning opstart', 'Lotnumre produceret', 'Slat tilgang',
                'Slat afgang', 'Rework tilgang', 'Rework afgang' ,'Prod.ordre status']

if get_section_status_code(df_results_generelt, get_section_visibility(df_sections, section_id)) == 99:
    try:
        df_results_generelt['Varenummer'] = '12345678'
        df_results_generelt['Varenavn'] = 'varenavn'
        df_results_generelt['Basisenhed'] = 'KRT'
        df_results_generelt['Receptnummer'] = '10401234'
        df_results_generelt['Produktionsdato'] = '2021-02-03,2021-02-04'
        df_results_generelt['Stregkode'] = '00000413547'
        df_results_generelt['Lotnumre produceret'] = '17'
        df_results_generelt['Slat tilgang'] = '5'
        df_results_generelt['Slat afgang'] = '5'
        df_results_generelt['Rework tilgang'] = '2'
        df_results_generelt['Rework afgang'] = '1'
        df_results_generelt['Prod.ordre status'] = 'Færdig'
        df_results_generelt = df_results_generelt[column_order]
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_results_generelt.transpose(), section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_results_generelt, get_section_visibility(df_sections, section_id)))


# =============================================================================
# Section 4: Mølleordrer
# =============================================================================
section_id = 4
section_name = get_section_name(section_id)
timestamp = datetime.now()
column_order = ['Receptnummer', 'Receptnavn', 'Dato', 'Mølle',
                'Probat id', 'Ordrenummer', 'Kilo']

if get_section_status_code(df_probat_ulg, get_section_visibility(df_sections, section_id)) == 99:
    try:
        df_probat_ulg['Receptnavn'] = 'Receptnavn'
        df_probat_ulg = df_probat_ulg[column_order]
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_probat_ulg, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_probat_ulg, get_section_visibility(df_sections, section_id)))


# =============================================================================
# Section 5: Risteordrer
# =============================================================================
section_id = 5
section_name = get_section_name(section_id)
timestamp = datetime.now()
column_order = ['Receptnummer', 'Receptnavn', 'Dato', 'Rister',
                'Probat id', 'Ordrenummer', 'Kilo']

if get_section_status_code(df_probat_ulr, get_section_visibility(df_sections, section_id)) == 99:
    try:
        df_probat_ulr['Receptnavn'] = 'Receptnavn'
        df_probat_ulr = df_probat_ulr[column_order]
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_probat_ulr, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_probat_ulr, get_section_visibility(df_sections, section_id)))



# =============================================================================
# Section 8: Massebalance
# =============================================================================
section_id = 8
section_name = get_section_name(section_id)
timestamp = datetime.now()
dict_massebalance = {'[1] Råkaffe': 100,
                     '[2] Ristet kaffe': 90,
                     '[3] Difference': 0,
                     '[4] Færdigvaretilgang': 88,
                     '[5] Difference': 0,
                     '[6] Salg': 83,
                     '[7] Kassation & ompak': 2,
                     '[8] Restlager': 3,
                     '[9] Difference': 0 }
dict_massebalance['[3] Difference'] = dict_massebalance['[1] Råkaffe'] - dict_massebalance['[2] Ristet kaffe']
dict_massebalance['[5] Difference'] = dict_massebalance['[2] Ristet kaffe'] - dict_massebalance['[4] Færdigvaretilgang']
dict_massebalance['[9] Difference'] = dict_massebalance['[1] Råkaffe'] - dict_massebalance['[2] Ristet kaffe']
df_massebalance = pd.DataFrame.from_dict(data=dict_massebalance, orient='index')

if get_section_status_code(df_massebalance, get_section_visibility(df_sections, section_id)) == 99:
    try:
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_massebalance, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_massebalance, get_section_visibility(df_sections, section_id)))


# =============================================================================
# Section 11: Ordrestatistik fra e-vejning
# =============================================================================
section_id = 11
section_name = get_section_name(section_id)
timestamp = datetime.now()
column_order = ['Total vægt', 'Antal enheder', 'Middelvægt', 'Standardafvigelse',
                'Gns. godvægt per enhed', 'Godvægt total', 'Nominel vægt', 'Taravægt']

if get_section_status_code(df_com_statistics, get_section_visibility(df_sections, section_id)) == 99:
    try:
        df_com_statistics = df_com_statistics[column_order]
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_com_statistics, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_com_statistics, get_section_visibility(df_sections, section_id)))


# =============================================================================
# Section 12: Karakterer
# =============================================================================
section_id = 12
section_name = get_section_name(section_id)
timestamp = datetime.now()

if get_section_status_code(df_karakterer, get_section_visibility(df_sections, section_id)) == 99:
    try:
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_karakterer, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_karakterer, get_section_visibility(df_sections, section_id)))



# =============================================================================
# Section 16: Reference- og henstandsprøver
# =============================================================================
section_id = 16
section_name = get_section_name(section_id)
timestamp = datetime.now()
column_order = ['Id', 'Registreringstidspunkt', 'Operatør', 'Silo', 'Prøvetype',
                'Bemærkning', 'Smagning status', 'Antal prøver']
df_temp = df_prøver[df_prøver['Prøvetype int'] != 0]

if get_section_status_code(df_temp, get_section_visibility(df_sections, section_id)) == 99:
    try:
        df_temp = df_temp[column_order]
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_temp, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_temp, get_section_visibility(df_sections, section_id)))


# =============================================================================
# Section 17: Udtagne kontrolprøver
# =============================================================================
section_id = 17
section_name = get_section_name(section_id)
timestamp = datetime.now()
column_order = ['Id','Registreringstidspunkt', 'Operatør', 'Bemærkning',
                'Mærkning', 'Rygsvejsning', 'Tæthed', 'Ventil', 'Peelbar',
                'Tintie', 'Vægt', 'Ilt']
df_temp = df_prøver[df_prøver['Prøvetype int'] == 0]

if get_section_status_code(df_temp, get_section_visibility(df_sections, section_id)) == 99:
    try:
        df_temp = df_temp[column_order]
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_temp, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_temp, get_section_visibility(df_sections, section_id)))


# =============================================================================
# Section 18: Sektionslog
# =============================================================================
section_id = 18
df_section_log = pd.read_sql(query_ds_section_log, con_04)
section_name = get_section_name(section_id)
timestamp = datetime.now()

if get_section_status_code(df_section_log, get_section_visibility(df_sections, section_id)) == 99:
    try:
        # Write results to Word and Excel
        insert_dataframe_into_excel (df_section_log, section_name)
        # *** TO DO: Insert into Word
        # Write status into log
        section_log_insert(timestamp, section_id, 0)
    except: # Insert error into log
        section_log_insert(timestamp, section_id, 2)
else: # Write into log if no data is found or section is out of scope
    section_log_insert(timestamp, section_id, get_section_status_code(df_section_log, get_section_visibility(df_sections, section_id)))


# =============================================================================
#
# Indsæt i Excel
# insert_dataframe_into_excel(df_results_generelt, 'Generelt')
#
# Nogenlunde indsæt i Word
# doc.add_paragraph('Test tekst!!!')
# doc.save(path_file_doc)
# =============================================================================


#Save files
excel_writer.save()
# *** TODO SAVE WORD DOCUMENT
# *** TODO SAVE PDF FILE


# =============================================================================
# # Dette må være flowet for dannelse af dataframes..
# if get_section_status_code(dataframe, get_section_visibility(dataframe, section)) == 99:
#     try:
#         # Forbered dataframe
#         # Skriv i Word dokument og Excel
#         # Skriv i Excel: insert_dataframe_into_excel (dataframe, sheetname)
#         section_log_insert(timestamp, section_code, 0)
#     except:
#         # Hvis fejl
#         section_log_insert(timestamp, section_code, 2)
# else:
#     # Skriv statuskode
#     section_log_insert(timestamp, section_code, get_section_status_code(Dataframe, get_section_visibility(df_sections, section_code)))
#
# =============================================================================
