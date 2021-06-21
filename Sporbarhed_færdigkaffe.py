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

server_probat = '192.168.125.161'
db_probat = 'BKI_IMP_EXP'
con_probat = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_probat};DATABASE={db_probat};uid=bki_read;pwd=Probat2016')
params_probat = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_probat};DATABASE={db_probat};Trusted_Connection=yes')
engine_probat = create_engine(f'mssql+pyodbc:///?odbc_connect={params_probat}')

# =============================================================================
# Read data from request
# =============================================================================
query_ds_request =  """SELECT TOP 1 [Id] ,[Forespørgselstype] ,[Produkttype]
                    ,[Rapporttype] ,[Rapport_modtager] ,[Rapport_pdf], [Ordrenummer]
                    ,[Rapport_excel] ,[Note_forespørgsel] FROM [trc].[Sporbarhed_forespørgsel]
                    WHERE [Forespørgsel_igangsat] IS NULL"""
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
req_pdf = df_request.loc[0, 'Rapport_pdf']
req_excel = df_request.loc[0, 'Rapport_excel']
req_recipients = df_request.loc[0, 'Rapport_modtager']
req_produkttype = df_request.loc[0, 'Produkttype'] # Behov for denne her?
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
                    SELECT SF.[Ordrenummer] ,SF.[Pakketidspunkt] ,KH.[Igangsat_af] AS [Igangsat af]
                    ,KH.[Silo_opstart] AS [Opstartssilo] ,KH.[Taravægt] ,KH.[Nitrogen]
                    ,KH.[Bemærkning] AS [Bemærkning opstart] ,ISNULL(KP.[Kontrolprøve] ,0) AS [Kontrolprøver]
                    ,ISNULL(KP.[Referenceprøve] ,0) AS [Referenceprøver]
                    ,ISNULL(KP.[Henstandsprøve] ,0) AS [Henstandsprøver]
                    ,CASE WHEN SK.[Status] = 1 THEN 'Godkendt' WHEN SK.[Status] = 0 THEN 'Afvist'
                    ELSE 'Ej smagt' END AS [Smagning status]
                    FROM [trc].[Sporbarhed_forespørgsel] AS SF
                    LEFT JOIN [cof].[Kontrolskema_hoved] AS KH ON SF.[Ordrenummer] = KH.[Ordrenummer]
                    LEFT JOIN [KP] ON SF.[Ordrenummer] = KP.[Ordrenummer]
                    LEFT JOIN [SK] ON SF.[Ordrenummer] = SK.[Referencenummer]
                    WHERE SF.[Id] = {req_id} """

query_ds_samples = f""" SELECT KP.[Ordrenummer],KP.[Registreringstidspunkt]
            	,KP.[Registreret_af],KP.[Bemærkning],KP.[Prøvetype] AS [Prøvetype int]
                ,P.[Beskrivelse] AS [Prøvetype]
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
                ,KP.[Vægt_aflæst],KP.[Kontrol_ilt],KP.[Silo]
                ,CASE WHEN SK.[Status] = 1 THEN 'Godkendt' WHEN SK.[Status] = 0
                THEN 'Afvist' ELSE 'Ej smagt' END AS [Smagning status]
                FROM [cof].[Kontrolskema_prøver] AS KP
                INNER JOIN [cof].[Prøvetype] AS P
                    ON KP.[Prøvetype] = P.[Id]
                LEFT JOIN [cof].[Smageskema] AS SK
                    ON KP.[Id] = SK.[Id_org]
                    AND SK.[Id_org_kildenummer] = 6
                WHERE KP.[Ordrenummer] = {req_order_no} """

query_nav_færdigvarer = f""" {req_order_no}
                        """

query_nav_vare = """ SELECT [No_] AS [Varenummer] ,[Description] AS [Navn]
                     FROM [dbo].[BKI foods a_s$Item]
                     WHERE [Item Category Code] IN ('FÆR KAFFE','RISTKAFFE','RÅKAFFE')
                     [No_] NOT LIKE '9%' """

# =============================================================================
# Variables based on queries above nessecary for queries below
# =============================================================================
Lotnumbers = ()
Probat_mølleordrer = ()

# =============================================================================
# Queries using variables based on previous queries
# =============================================================================
query_nav_customers = f""" {Lotnumbers} """

query_probat_mølleordrer = f""" SELECT DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                                ,[PRODUCTION_ORDER_ID] AS [Probat id]
                                ,[SOURCE_NAME] AS [Mølle] ,[ORDER_NAME] AS [Ordrenummer]
                            	,[D_CUSTOMER_CODE] AS [Receptnummer]
                            	,SUM([WEIGHT]) / 1000.0 AS [Kilo]
                                FROM [dbo].[PRO_EXP_ORDER_UNLOAD_G]
                                WHERE [ORDER_NAME] IN {Probat_mølleordrer}
                                GROUP BY 
                            	DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                            	,[PRODUCTION_ORDER_ID] ,[SOURCE_NAME]
                            	,[ORDER_NAME] ,[D_CUSTOMER_CODE] """

df_results_generelt = pd.read_sql(query_ds_generelt, con_04) # 0
df_prøver = pd.read_sql(query_ds_samples, con_04)


# Get visibility for section from query
def get_section_visibility(dataframe, section):
    return dataframe['Sektion_synlig'].iloc[section]

# Get section name for section from query
def get_section_name(section):
    x = df_sections['Sektion navn'].iloc[section]
    if len(x) == 0:
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
    dataframe.to_excel(path_file_wb, sheet_name=sheetname)
    
# =============================================================================
# Section 0: Generelt
# =============================================================================
section_id = 0
timestamp = datetime.now()
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
        # Skriv i Word dokument og Excel
        insert_dataframe_into_excel (df_results_generelt.transpose(), get_section_name(section_id))
        section_log_insert(timestamp, section_id, 0)
    except: # Statuskode hvis fejl opstår
        # Hvis fejl
        section_log_insert(timestamp, section_id, 2)
else: # Statuskode hvis ingen data eller sektion fravalgt og ingen fejl er opstået
    section_log_insert(timestamp, section_id, get_section_status_code(df_results_generelt, get_section_visibility(df_sections, section_id)))
    



# =============================================================================
# 
# Indsæt i Excel    
# insert_dataframe_into_excel(df_results_generelt, 'Generelt')
# 
# Nogenlunde indsæt i Word
# doc.add_paragraph('Test tekst!!!')
# doc.save(path_file_doc)
# =============================================================================



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
