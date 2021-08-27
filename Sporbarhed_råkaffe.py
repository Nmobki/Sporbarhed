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
    x = df_sections['Sektion navn'].iloc[section-1]
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
cursor_04.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                  SET [Forespørgsel_igangsat] = getdate()
                  WHERE [Id] = {req_id} """)
cursor_04.commit()
log_insert(script_name, f'Request id: {req_id} initiated')

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
    pass

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
pd.DataFrame(data=dict_email_log, index=[0]).to_sql('Sporbarhed_email_log', con=engine_04, schema='trc', if_exists='append', index=False)
log_insert(script_name, f'Request id: {req_id} inserted into [trc].[Email_log]')

# =============================================================================
# Update request that dataprocessing has been completed
# =============================================================================
cursor_04.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                  SET Data_færdigbehandlet = 1
                  WHERE [Id] = {req_id}""")
cursor_04.commit()
log_insert(script_name, f'Request id: {req_id} completed')

# Exit script
raise SystemExit(0)