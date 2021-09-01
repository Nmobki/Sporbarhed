#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import urllib
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import docx
from docx.shared import Inches
import openpyxl
import networkx as nx


# =============================================================================
# Define functions
# =============================================================================

# Get section name for section from query
def get_section_name(section):
    df_temp_sections = df_sections.loc[df_sections['Sektion'] == section]
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
def section_log_insert(section, statuscode, errorcode=None):
    df = pd.DataFrame(data={'Forespørgsels_id':req_id,
                            'Sektion':section,
                            'Statuskode':statuscode,
                            'Fejlkode_script':str(errorcode)}
                      , index=[0])
    df.to_sql('Sporbarhed_sektion_log', con=engine_04, schema='trc', if_exists='append', index=False)

# Write dataframe into Excel sheet
def insert_dataframe_into_excel (dataframe, sheetname, include_index):
    dataframe.to_excel(excel_writer, sheet_name=sheetname, index=include_index)

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
    dict = {'None':None,'Zero':0}
    if denominator in [0,None]:
        return dict[zero_return]
    else:
        return nominator / denominator

# Write into dbo.log
def log_insert(event, note):
    dict_log = {'Note': note
                ,'Event': event}
    pd.DataFrame(data=dict_log, index=[0]).to_sql('Log', con=engine_04, schema='dbo', if_exists='append', index=False)

# Get info from item table in Navision
def get_nav_item_info(item_no, field):
    df_temp = df_nav_items[df_nav_items['Nummer'] == item_no]
    return df_temp[field].iloc[0]


# Convert placeholder values from dataframe to empty string for Word document
def convert_placeholders_word(string):
    if string in ['None','nan','NaT']:
        return ''
    else:
        return string

# Add dataframe to word document
def add_section_to_word(dataframe, section, pagebreak, rows_to_bold):
    # Add section header
    doc.add_heading(section, 1)
    # Add a table with an extra row for headers
    table = doc.add_table(dataframe.shape[0]+1, dataframe.shape[1])
    table.style = 'Table Grid'
    # Add headers to top row
    for i in range(dataframe.shape[-1]):
        table.cell(0,i).text = dataframe.columns[i]
    # Add data from dataframe to the table, replace supposed blank cells using function
    for x in range(dataframe.shape[0]):
        for y in range(dataframe.shape[-1]):
            table.cell(x+1,y).text =  convert_placeholders_word(str(dataframe.values[x,y]))
    # Bold total row if it exists
    for y in rows_to_bold:
        for x in range(dataframe.shape[1]):
            table.rows[y].cells[x].paragraphs[0].runs[0].font.bold = True
    # Add page break
    if pagebreak:
        doc.add_page_break()


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
# Read data from request
# =============================================================================
query_ds_request =  """ SELECT TOP 1 [Id] ,[Forespørgselstype],[Rapport_modtager]
                    ,[Referencenummer] ,[Note_forespørgsel] ,[Modtagelse] 
                    FROM [trc].[Sporbarhed_forespørgsel]
                    WHERE [Forespørgsel_igangsat] IS NULL """
df_request = pd.read_sql(query_ds_request, con_04)

# Exit script if no request data is found
if len(df_request) == 0:
    raise SystemExit(0)

# =============================================================================
# Set request variables
# =============================================================================
req_type = df_request.loc[0, 'Forespørgselstype']
req_reference_no = df_request.loc[0, 'Referencenummer']
req_recipients = df_request.loc[0, 'Rapport_modtager']
req_note = df_request.loc[0, 'Note_forespørgsel']
req_id = df_request.loc[0, 'Id']
req_modtagelse = df_request.loc[0, 'Modtagelse']

script_name = 'Sporbarhed_samlet.py'
timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
orders_top_level = [req_reference_no]
orders_related = []

# =============================================================================
# Update request that it is initiated and write into log
# =============================================================================
# =============================================================================
# cursor_04.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
#                   SET [Forespørgsel_igangsat] = getdate()
#                   WHERE [Id] = {req_id} """)
# cursor_04.commit()
# =============================================================================
# log_insert(script_name, f'Request id: {req_id} initiated')

# =============================================================================
# Variables for files generated
# =============================================================================
filepath = r'\\filsrv01\BKI\11. Økonomi\04 - Controlling\NMO\4. Kvalitet\Sporbarhedstest\Tests via PowerApps'
file_name = f'Rapport_{req_reference_no}_{req_id}'

doc = docx.Document()
doc.add_heading(f'Rapport for produktionsordre {req_reference_no}',0)
doc.sections[0].header.paragraphs[0].text = f'{script_name}'
doc.sections[0].footer.paragraphs[0].text = f'{timestamp}'
doc.sections[0].page_width = docx.shared.Mm(297)
doc.sections[0].page_height = docx.shared.Mm(210)
doc.sections[0].top_margin = docx.shared.Mm(15)
doc.sections[0].bottom_margin = docx.shared.Mm(15)
doc.sections[0].left_margin = docx.shared.Mm(10)
doc.sections[0].right_margin = docx.shared.Mm(10)
doc.sections[0].orientation = docx.enum.section.WD_ORIENT.LANDSCAPE

doc_name = f'{file_name}.docx'
path_file_doc = filepath + r'\\' + doc_name

wb = openpyxl.Workbook()
wb_name = f'{file_name}.xlsx'
path_file_wb = filepath + r'\\' + wb_name
excel_writer = pd.ExcelWriter(path_file_wb, engine='xlsxwriter')

png_relations_name = f'{file_name}.png'
path_png_relations = filepath + r'\\' + png_relations_name

# =============================================================================
# Read setup for section for reporttypes. NAV querys with NOLOCK to prevent deadlocks
# =============================================================================
query_ds_reporttypes =  f"""SELECT SRS.[Sektion], SS.[Beskrivelse] AS [Sektion navn]
                       FROM [trc].[Sporbarhed_rapport_sektion] AS SRS
					   INNER JOIN [trc].[Sporbarhed_sektion] AS SS
					   ON SRS.[Sektion] = SS.[Id]
                       WHERE [Forespørgselstype] = {req_type} """
df_sections = pd.read_sql(query_ds_reporttypes, con_04)

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

class rapport_råkaffe:
    # General info from Navision
    query__nav_generelt = f""" SELECT TOP 1 PL.[Buy-from Vendor No_] AS [Leverandørnummer]
                    	,V.[Name] AS [Leverandørnavn] ,PL.[No_] AS [Varenummer]
                        ,I.[Description] AS [Varenavn] ,I.[Mærkningsordning]
                        FROM [dbo].[BKI foods a_s$Purchase Line] AS PL
                        INNER JOIN [dbo].[BKI foods a_s$Item] AS I
                            ON PL.[No_] = I.[No_]
                        LEFT JOIN [dbo].[BKI foods a_s$Vendor] AS V
                            ON PL.[Buy-from Vendor No_] = V.[No_]
                        WHERE PL.[Type] = 2
                            AND PL.[Document No_] = '{req_reference_no}' """
    df_nav_generelt = pd.read_sql(query__nav_generelt, con_nav)
    
    # Get timestamp for last export of inventory from Probat
    query_probat_inventory_timestamp = """ WITH [Tables] AS (
                                       SELECT MAX([RECORDING_DATE]) AS [Date]
                                        FROM [dbo].[PRO_EXP_PRODUCT_POS_INVENTORY]
                                        UNION ALL
                                        SELECT MAX([RECORDING_DATE])
                                        FROM [dbo].[PRO_EXP_WAREHOUSE_INVENTORY] )
                                        SELECT MIN([Date]) AS [Silobeholdning eksporteret]
                                        FROM [Tables] """
    df_probat_inventory_timestamp = pd.read_sql(query_probat_inventory_timestamp, con_probat)
    
    # Information from Probat for the receiving of coffee
    query_probat_receiving = f""" IF '{req_modtagelse}' = 'None' -- Modtagelse ikke tastet
                             BEGIN
                             SELECT	CAST([DESTINATION] AS VARCHAR(20)) AS [Placering]
                             ,[RECORDING_DATE] AS [Dato] ,[PAPER_VALUE] / 10.0 AS [Kilo]
                    		 ,NULL AS [Restlager]
                             FROM [dbo].[PRO_EXP_REC_ARRIVE]
                        	 WHERE CAST([CONTRACT_NO] AS VARCHAR(20)) = '{req_reference_no}'
                        	 UNION ALL
                        	 SELECT [Placering] ,NULL ,NULL ,SUM([Kilo]) AS [Kilo]
                        	 FROM [dbo].[Newest total inventory]
                        	 WHERE [Kontrakt] = '{req_reference_no}' 
                             AND [Placering] NOT LIKE '2__'
                        	 GROUP BY [Placering]
                             END
                             IF '{req_modtagelse}' <> 'None' -- Modtagelse er udfyldt
                             BEGIN
                             SELECT CAST([DESTINATION] AS VARCHAR(20)) AS [Placering]
                             ,[RECORDING_DATE] AS [Dato] ,[PAPER_VALUE] / 10.0 AS [Kilo]
                             ,NULL AS [Restlager]
                             FROM [dbo].[PRO_EXP_REC_ARRIVE]
                             WHERE CAST([CONTRACT_NO] AS VARCHAR(20)) = '{req_reference_no}'
                             AND CAST([DELIVERY_NAME] AS VARCHAR(20)) = '{req_modtagelse}'
                             UNION ALL
                             SELECT [Placering] ,NULL ,NULL ,SUM([Kilo]) AS [Kilo]
                             FROM [dbo].[Newest total inventory]
                             WHERE [Kontrakt] = '{req_reference_no}' 
                             AND CAST([Modtagelse] AS VARCHAR(20)) = '{req_modtagelse}'
                             AND [Placering] NOT LIKE '2__'
                             GROUP BY [Placering] END """
    df_probat_receiving = pd.read_sql(query_probat_receiving, con_probat)
    
    # Information from Probat for the processing of coffee
    query_probat_processing = f""" IF 'None' = 'None' -- Ingen modtagelse tastet
                              BEGIN
                              SELECT [DESTINATION] AS [Silo]
                              ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ), 0) AS [Dato]
                              ,SUM([WEIGHT] / 10.0) AS [Kilo]
                              ,0 AS [Restlager]
                              FROM [dbo].[PRO_EXP_REC_SUM_DEST]
                              WHERE [CONTRACT_NO] = '{req_reference_no}' AND [DESTINATION] LIKE '2__'
                              GROUP BY [DESTINATION] ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ) ,0)
                              UNION ALL
                              SELECT [Placering] ,NULL ,0 ,SUM([Kilo])
                              FROM [dbo].[Newest total inventory]
                              WHERE [Kontrakt] = '{req_reference_no}' AND [Placering]  LIKE '2__'
                              GROUP BY [Placering]
                              END
                              IF 'None' <> 'None' -- Modtagelse tastet
                              BEGIN
                              SELECT [DESTINATION] AS [Silo]
                              ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ), 0) AS [Dato]
                              ,SUM([WEIGHT] / 10.0) AS [Kilo]
                              ,0 AS [Restlager]
                              FROM [dbo].[PRO_EXP_REC_SUM_DEST]
                              WHERE [CONTRACT_NO] = '{req_reference_no}'
                              AND [DESTINATION] LIKE '2__'
                              AND [DELIVERY_NAME] = '{req_modtagelse}'
                              GROUP BY [DESTINATION] ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ) ,0)
                              UNION ALL
                              SELECT [Placering] ,NULL ,0
                              ,SUM([Kilo]) AS [Kilo]
                              FROM [dbo].[Newest total inventory]
                              WHERE [Kontrakt] = '{req_reference_no}' AND [Placering]  LIKE '2__'
                              AND [Modtagelse] = '{req_modtagelse}'
                              GROUP BY [Placering]
                              END """
    df_probat_processing = pd.read_sql(query_probat_processing, con_probat)
    
    # Get order numbers the requested coffee has been used in
    query_probat_used_in_roast = f""" IF 'None' = 'None' -- Ingen modtagelse tastet
                           BEGIN
                           SELECT [ORDER_NAME]
                           FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                           WHERE [S_CONTRACT_NO] = '{req_reference_no}'
                           GROUP BY [ORDER_NAME]
                           END
                           IF 'None' <> 'None' -- Modtagelse tastet
                           BEGIN
                           SELECT [ORDER_NAME]
                           FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                           WHERE [S_CONTRACT_NO] = '{req_reference_no}'
                           AND [S_DELIVERY_NAME] = '{req_modtagelse}'
                           GROUP BY [ORDER_NAME]
                           END """
    df_probat_used_in_roast = pd.read_sql(query_probat_used_in_roast, con_probat)
    # Convert orders to string for use in later queries
    roast_orders = df_probat_used_in_roast['ORDER_NAME'].unique().tolist()
    sql_roast_orders = string_to_sql(roast_orders)

    query_probat_roast_input = f""" IF 'None' = 'None'
                                      BEGIN
                                      SELECT [CUSTOMER_CODE] AS [Varenummer]
                                      ,[ORDER_NAME] AS [Ordrenummer]
                                      ,[DESTINATION] AS [Rister]
                                      ,SUM(CASE WHEN [S_CONTRACT_NO] = '{req_reference_no}'
                                          THEN [WEIGHT] / 1000.0 ELSE 0 END) 
                                          AS [Heraf kontrakt]
                                      ,SUM([WEIGHT] / 1000.0) AS [Kilo råkaffe]
                                      FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                                      WHERE [ORDER_NAME] IN ({sql_roast_orders})
                                      GROUP BY  [CUSTOMER_CODE] 
                                      ,DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                                      ,[ORDER_NAME] ,[DESTINATION]
                                      END
                                      IF 'None' <> 'None'
                                      BEGIN
                                      SELECT [CUSTOMER_CODE] AS [Varenummer]
                                      ,[ORDER_NAME] AS [Ordrenummer]
                                      ,[DESTINATION] AS [Rister]
                                      ,SUM(CASE WHEN [S_CONTRACT_NO] = '{req_reference_no}' 
                                           AND [S_DELIVERY_NAME] = '{req_modtagelse}'
                                    	   THEN [WEIGHT] / 1000.0
                                    	   ELSE 0 END) AS [Heraf kontrakt]
                                      ,SUM([WEIGHT] / 1000.0) AS [Kilo råkaffe]
                                      FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                                      WHERE [ORDER_NAME] IN ({sql_roast_orders})
                                      GROUP BY  [CUSTOMER_CODE]
                                      ,DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                                      ,[ORDER_NAME] ,[DESTINATION] END """
    if len(sql_roast_orders) > 0:
        df_probat_roast_input = pd.read_sql(query_probat_roast_input, con_probat)
    else:
        df_probat_roast_input = pd.DataFrame()
        
    query_probat_roast_output = f""" SELECT	
                                DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                            	,[DEST_NAME] AS [Silo] ,[ORDER_NAME] AS [Ordrenummer]
                            	,SUM([WEIGHT]) / 1000.0 AS [Kilo ristet]
                                FROM [dbo].[PRO_EXP_ORDER_UNLOAD_R]
                                WHERE [ORDER_NAME] IN ({sql_roast_orders})
                                GROUP BY DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                                ,[DEST_NAME] ,[ORDER_NAME] """
    if len(sql_roast_orders) > 0:
        df_probat_roast_output = pd.read_sql(query_probat_roast_output, con_probat)
    else:
        df_probat_roast_output = pd.DataFrame()

    # =============================================================================
    # Section 1: Generelt
    # =============================================================================
    section_id = 1
    section_name = get_section_name(section_id)
    column_order = ['Kontraktnummer','Modtagelse','Varenummer','Varenavn','Mærkningsordning','Leverandørnummer'
                    ,'Leverandørnavn','Silobeholdning eksporteret']

    if get_section_status_code(df_nav_generelt) == 99:
        try:
            df_nav_generelt['Kontraktnummer'] = req_reference_no
            df_nav_generelt['Modtagelse'] = req_modtagelse
            df_nav_generelt['Silobeholdning eksporteret'] = df_probat_inventory_timestamp['Silobeholdning eksporteret'].iloc[0]
            # Apply column formating
            df_nav_generelt['Silobeholdning eksporteret'] = df_nav_generelt['Silobeholdning eksporteret'].dt.strftime('%d-%m-%Y %H:%M')
            # Transpose dataframe
            df_nav_generelt = df_nav_generelt[column_order].transpose()
            df_nav_generelt = df_nav_generelt.reset_index()
            df_nav_generelt.columns = ['Sektion','Værdi']
            # Write results to Word and Excel
            insert_dataframe_into_excel(df_nav_generelt, section_name, True)
            add_section_to_word(df_nav_generelt, section_name, True, [0])
            # Write status into log
            section_log_insert(section_id, 0)
        except Exception as e: # Insert error into log
            section_log_insert(section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        section_log_insert(section_id, get_section_status_code(df_nav_generelt))

    # =============================================================================
    # Section 21: Modtagelse
    # =============================================================================
    section_id = 21
    section_name = get_section_name(section_id)
    column_order = ['Placering','Dato','Kilo','Restlager']
    columns_0_dec = ['Kilo','Restlager']

    if get_section_status_code(df_probat_receiving) == 99:
        try:
            # Create total for dataframe
            dict_modtagelse_total = {'Kilo': [df_probat_receiving['Kilo'].sum()],
                                     'Restlager': [df_probat_receiving['Restlager'].sum()]}
            # Create temp dataframe including total
            df_temp_total = pd.concat([df_probat_receiving,
                                       pd.DataFrame.from_dict(data=dict_modtagelse_total, orient = 'columns')])
            # Apply column formating
            df_temp_total['Dato'] = df_temp_total['Dato'].dt.strftime('%d-%m-%Y')
            for col in columns_0_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: number_format(x, 'dec_0'))
            df_temp_total = df_temp_total[column_order]
            # Write results to Word and Excel
            insert_dataframe_into_excel(df_temp_total, section_name, False)
            add_section_to_word(df_temp_total, section_name, True, [-1,0])
            # Write status into log
            section_log_insert(section_id, 0)
        except Exception as e: # Insert error into log
            section_log_insert(section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        section_log_insert(section_id, get_section_status_code(df_temp_total))

    # =============================================================================
    # Section 20: Rensning
    # =============================================================================
    section_id = 20
    section_name = get_section_name(section_id)
    column_order = ['Silo','Dato','Kilo','Restlager']
    columns_0_dec = ['Kilo','Restlager']
    if get_section_status_code(df_probat_processing) == 99:
        try:
            # Apply column formating for date column before concat
            df_probat_processing['Dato'] = df_probat_processing['Dato'].dt.strftime('%d-%m-%Y')
            df_probat_processing.fillna('', inplace=True)
            #Concat dates into one strng and sum numeric columns if they can be grouped
            df_probat_processing = df_probat_processing.groupby('Silo').agg(
                {'Kilo': 'sum',
                 'Restlager': 'sum',
                 'Dato': lambda x: ','.join(sorted(pd.Series.unique(x)))
                 }).reset_index()
            df_probat_processing['Dato'] = df_probat_processing['Dato'].apply(lambda x: x.rstrip(','))
            df_probat_processing['Dato'] = df_probat_processing['Dato'].apply(lambda x: x.lstrip(','))
            # Create total for dataframe
            dict_modtagelse_total = {'Kilo': [df_probat_processing['Kilo'].sum()],
                                     'Restlager': [df_probat_processing['Restlager'].sum()]}
            # Create temp dataframe including total
            df_temp_total = pd.concat([df_probat_processing,
                                       pd.DataFrame.from_dict(data=dict_modtagelse_total, orient = 'columns')])
            for col in columns_0_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: number_format(x, 'dec_0'))
            df_temp_total = df_temp_total[column_order]
            # Write results to Word and Excel
            insert_dataframe_into_excel(df_temp_total, section_name, False)
            add_section_to_word(df_temp_total, section_name, True, [-1,0])
            # Write status into log
            section_log_insert(section_id, 0)
        except Exception as e: # Insert error into log
            section_log_insert(section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        section_log_insert(section_id, get_section_status_code(df_temp_total))

    # =============================================================================
    # Section 5: Risteordrer
    # =============================================================================
    section_id = 5
    section_name = get_section_name(section_id)
    column_order = ['Varenummer','Varenavn','Dato','Rister','Ordrenummer','Silo',
                    'Kilo råkaffe','Heraf kontrakt','Kilo ristet']
    columns_0_dec = ['Kilo råkaffe','Heraf kontrakt','Kilo ristet']

    if get_section_status_code(df_probat_roast_output) == 99:
        try:
            # Apply column formating for date column before concat
            df_probat_roast_output['Dato'] = df_probat_roast_output['Dato'].dt.strftime('%d-%m-%Y')
            df_probat_roast_output.fillna('', inplace=True)
            # Concat dates into one strng and sum numeric columns if they can be grouped
            df_probat_roast_output = df_probat_roast_output.groupby('Ordrenummer').agg(
                {'Kilo ristet': 'sum',
                 'Dato': lambda x: ','.join(sorted(pd.Series.unique(x))),
                 'Silo': lambda x: ','.join(sorted(pd.Series.unique(x)))
                 }).reset_index()
            # Join roast output to input for one table
            df_probat_roast_total = pd.merge(df_probat_roast_input, 
                                             df_probat_roast_output,
                                             left_on = 'Ordrenummer', 
                                             right_on = 'Ordrenummer',
                                             how = 'left',
                                             suffixes = ('' ,'_R')
                                             )
            #Column formating and lookups
            df_probat_roast_total['Dato'] = df_probat_roast_total['Dato'].apply(lambda x: x.rstrip(','))
            df_probat_roast_total['Dato'] = df_probat_roast_total['Dato'].apply(lambda x: x.lstrip(','))
            df_probat_roast_total['Varenavn'] = df_probat_roast_total['Varenummer'].apply(get_nav_item_info, field='Beskrivelse')
            # Create total for dataframe
            dict_risteordrer_total = {'Kilo råkaffe': df_probat_roast_total['Kilo råkaffe'].sum(),
                                     'Heraf kontrakt': df_probat_roast_total['Heraf kontrakt'].sum(),
                                     'Kilo ristet': df_probat_roast_total['Kilo ristet'].sum()
                                     }

            # Create temp dataframe including total
            df_total_temp = pd.DataFrame([dict_risteordrer_total])
            df_temp_total = pd.concat([df_probat_roast_total,
                                   pd.DataFrame([dict_risteordrer_total])])
            for col in columns_0_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: number_format(x, 'dec_0'))
            df_temp_total = df_temp_total[column_order]
            # Write results to Word and Excel
            insert_dataframe_into_excel(df_temp_total, section_name, False)
            add_section_to_word(df_temp_total, section_name, True, [-1,0])
            # Write status into log
            section_log_insert(section_id, 0)
        except Exception as e: # Insert error into log
            section_log_insert(section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        section_log_insert(section_id, get_section_status_code(df_temp_total))





    #Save files
    excel_writer.save()
    # log_insert(script_name, f'Excel file {file_name} created')

    doc.save(path_file_doc)
    # log_insert(script_name, f'Word document {file_name} created')








if req_type == 0:
    pass
elif req_type == 1:
    pass
elif req_type == 2:
    rapport_råkaffe()

# =============================================================================
# Write into email log
# =============================================================================
dict_email_log = {'Filsti': filepath
                  ,'Filnavn': file_name
                  ,'Modtager': req_recipients
                  ,'Emne': f'Anmodet rapport for ordre {req_reference_no}'
                  ,'Forespørgsels_id': req_id
                  ,'Note':req_note}
# pd.DataFrame(data=dict_email_log, index=[0]).to_sql('Sporbarhed_email_log', con=engine_04, schema='trc', if_exists='append', index=False)
# log_insert(script_name, f'Request id: {req_id} inserted into [trc].[Email_log]')

# =============================================================================
# Update request that dataprocessing has been completed
# =============================================================================
cursor_04.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                  SET Data_færdigbehandlet = 1
                  WHERE [Id] = {req_id}""")
cursor_04.commit()
# log_insert(script_name, f'Request id: {req_id} completed')

# Exit script
raise SystemExit(0)
