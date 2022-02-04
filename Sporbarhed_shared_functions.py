#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import Sporbarhed_shared_server_information as sssi
from datetime import datetime
import sys


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
def get_connection(connection: str):
    """Returns connection string for requested database"""
    dictionary = {
        'navision': sssi.con_nav
        ,'comscale': sssi.con_comscale
        ,'probat': sssi.con_probat
        ,'bki_datastore': sssi.con_ds }
    return dictionary[connection]

# Get cursor
def get_cursor(connection: str):
    """Returns cursor for requested database"""
    dictionary = {
        'bki_datastore': sssi.cursor_ds }
    return dictionary[connection]

# Get filepath
def get_filepath(path_type: str):
    """Returns filepath for requested type"""
    dictionary = {
        'report': sssi.report_filepath}
    return dictionary[path_type]

# Get engines for connections
def get_engine(connection: str):
    """Returns engine for requested database"""
    dictionary = {
        'bki_datastore': sssi.engine_ds }
    return dictionary[connection]

# Check if script is supposed to exit. 0 value = exit
def get_exit_check(value: int):
    """Calls sys.exit() if input value == 0"""
    if value == 0:
        sys.exit()
    else:
        pass

# Read section names
def get_ds_reporttype(request_type: int):
    """Returns pandas dataframe with section id and name for requested reporttype"""
    query =  f"""SELECT SRS.[Sektion], SS.[Beskrivelse] AS [Sektion navn]
                       FROM [trc].[Sporbarhed_rapport_sektion] AS SRS
					   INNER JOIN [trc].[Sporbarhed_sektion] AS SS
					   ON SRS.[Sektion] = SS.[Id]
                       WHERE [Forespørgselstype] = {request_type} """
    return pd.read_sql(query, con_ds)

# Get information from section log
def get_ds_section_log(request_id: int):
    """Returns pandas dataframe with info from BKI_Datastore section log
       For the requested request_id"""
    query = f""" SELECT	SL.[Sektion] AS [Sektionskode],S.[Beskrivelse] AS [Sektion]
                ,SS.[Beskrivelse] AS [Status]
                ,SL.[Fejlkode_script] AS [Fejlkode script], SL.[Registreringstidspunkt]
                FROM [trc].[Sporbarhed_sektion_log] AS SL
                INNER JOIN [trc].[Sporbarhed_sektion] AS S
                    ON SL.[Sektion] = S.[Id]
                INNER JOIN [trc].[Sporbarhed_statuskode] AS SS
                    ON SL.[Statuskode] = SS.[Id]
                WHERE SL.[Forespørgsels_id] = {request_id} """
    return pd.read_sql(query, con_ds)

# Get section name for section from query
def get_section_name(section: int, dataframe) -> str:
    """
    Parameters
    ----------
    section : Int
        Id of the requested section.
    dataframe : Pandas dataframe
        Pandas dataframe containing information about section ids and names.
        Dataframe can be obtained using function get_ds_reporttype.
        Dataframe id column must be named 'Sektion', name column 'Sektion navn'
    \n Returns
    -------
    String containing name of section.
    If name of section contains more than 31 characters 'Sektion [id]' is returned instead.
    """
    df_temp_sections = dataframe.loc[dataframe['Sektion'] == section]
    x = df_temp_sections['Sektion navn'].iloc[0]
    if len(x) == 0 or len(x) > 31:
        return 'Sektion ' + str(section)
    else:
        return x

# Find statuscode for section log
def get_section_status_code(dataframe) -> int:
    """ input parameter == Pandas dataframe \n
        Returns 99 if dataframe contains data, otherwise return  == 1"""
    if len(dataframe) == 0:
        return 1 # Empty dataframe
    else:
        return 99 # Continue

# Concatenate and extend list of orders based on chosen traceability type
# 0 = all | 1 = just Probat | 2 = just Navision
def extend_order_list(relationship_type: int, original_list: list, probat_list: list, navision_list : list) -> list:
    """
    Extends list of orders with any new orders from additional lists depending on relationship_type requested
    \n Parameters
    ----------
    relationship_type : int
        Values 0-2. Defines whether list should contain only Probat or Navision orders or both.
        0 = all | 1 = just Probat | 2 = just Navision
    original_list : list
        Input list that needs to be extended.
    probat_list : list
        List containing Probat orders.
    navision_list : list
        List containing Navision orders.
    \n Returns
    -------
    original_list : list
        Returns original list extended with new orders from Probat and Navision lists depending on input relationship type.
    """
    dictionary = {
        0: navision_list + probat_list
        ,1: probat_list
        ,2: navision_list }
    temp_list = dictionary[relationship_type]
    for order in temp_list:
        if order not in original_list and order != '':
            original_list.append(order)
    return original_list

# Write into section log
def section_log_insert(request_id: int, section: int, statuscode: int, errorcode=None):
    """
    Writes into BKI_Datastore trc.section_log. \n
    \n Parameters
    ----------
    request_id : int
        Id of the requested being procesed in script calling function.
    section : int
        Section id being processed.
    statuscode : int
        Failure, success, no data etc..
    errorcode : str, optional
        Optional parameter to log any errorcodes returned by Python. The default is None.
    """
    df = pd.DataFrame(data={'Forespørgsels_id':request_id,
                            'Sektion':section,
                            'Statuskode':statuscode,
                            'Fejlkode_script':str(errorcode)}
                      , index=[0])
    df.to_sql('Sporbarhed_sektion_log', con=engine_ds, schema='trc', if_exists='append', index=False)

# Write dataframe into Excel sheet
def insert_dataframe_into_excel (engine, dataframe, sheetname: str, include_index: bool):
    """
    Inserts a dataframe into an Excel sheet
    \n Parameters
    ----------
    engine : Excel engine
    dataframe : Pandas dataframe
        Dataframe containing data supposed to be inserted into the Excel workbook.
    sheetname : str (max length 31 characters)
        Name of sheet created where dataframe will be inserted into.
    include_index : bool
        True if index is supposed to be included in insert into Excel, False if not.
    """
    dataframe.to_excel(engine, sheet_name=sheetname, index=include_index)

# Convert list into string for SQL IN operator
def string_to_sql(list_with_values: list) -> str:
    """
    Convert list of values into a single string which can be used for SQL queries IN clauses.
    Input ['a','b','c'] --> Output 'a','b','c'
    \n Parameters
    ----------
    list_with_values : list
        List containing all values which need to be joined into one string

    \n Returns
    -------
    String with comma separated values.
    Returned values are encased in '' when returned.
    """
    if len(list_with_values) == 0:
        return ''
    else:
        return "'{}'".format("','".join(list_with_values))

def number_format(value, number_type: str) -> str:
    """Converts an input number to a danish formated number.
       Number is returned as a string."""
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
def zero_division(nominator, denominator, zero_return: str):
    """ To be used on division to prevent division by zero error.
        zero_return is used to defined whether a 0 or None is desired to be returned incase of a zero value denominator."""
    dictionary = {'None':None,'Zero':0}
    if denominator in [0,None]:
        return dictionary[zero_return]
    else:
        return nominator / denominator

# Strip comma from commaseparated strings
def strip_comma_from_string(text: str) -> str:
    """ Strips input string of any commas if they are left- or rightmost character in the string."""
    text = text.rstrip(',')
    text = text.lstrip(',')
    return text

# Convert dates between formats
def convert_date_format(date, existing_format: str, new_format: str):
    """ Convert a date from one format to another.
        Currently conversion between 'yyyy-mm-dd' and 'dd-mm-yyyy' is possible."""
    if date is None:
        new_date = None
    elif existing_format == 'yyyy-mm-dd' and new_format == 'dd-mm-yyyy':
        new_date = datetime.strptime(date, '%Y-%m-%d').strftime('%d-%m-%Y')
    elif  existing_format == 'dd-mm-yyyy' and new_format == 'yyyy-mm-dd':
        new_date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
    return new_date

# Write into dbo.log
def log_insert(event: str, note: str):
    """Inserts a record into BKI_Datastore dbo.log with event and note."""
    dict_log = {'Note': note
                ,'Event': event}
    pd.DataFrame(data=dict_log, index=[0]).to_sql('Log', con=engine_ds, schema='dbo', if_exists='append', index=False)

# Get info from item table in Navision
# Query for Navision items, used for adding information to item numbers not queried directly from Navision
query_nav_items = """ SELECT I.[No_] AS [Nummer],I.[Description] AS [Beskrivelse]
                  ,I.[Item Category Code] AS [Varekategorikode]
				  ,CASE WHEN I.[Display Item] = 1 THEN 'Display'
				  WHEN I.[Item Category Code] = 'FÆR KAFFE' THEN 'Færdigkaffe'
				  WHEN I.[No_] LIKE '1040%' THEN 'Ristet kaffe'
				  WHEN I.[No_] LIKE '1050%' THEN 'Formalet kaffe'
				  WHEN I.[No_] LIKE '1020%' THEN 'Råkaffe'
				  ELSE I.[Item Category Code] END AS [Varetype]
				  ,CASE WHEN I.[Produktionskode] LIKE '%HB%' THEN 'Helbønne' ELSE 'Formalet' END AS [Kaffetype]
				  ,I.[Base Unit of Measure] AS [Basisenhed]
				  ,I.[Vendor No_] AS [Leverandørnummer]
				  ,CAST(PRI.[COLOR] AS INT) AS [Farve] 
				  ,PRI.[HUMIDITY] / 100.0 AS [Vandprocent]
                  FROM [dbo].[BKI foods a_s$Item] AS I
				  LEFT JOIN [dbo].[BKI foods a_s$PROBAT Item] AS PRI
				  ON I.[No_] = PRI.[CUSTOMER_CODE] """
df_nav_items = pd.read_sql(query_nav_items, con_nav)

def get_nav_item_info(item_no: str, field: str):
    """Returns information for the requested item number and field from Navision. """
    if item_no in df_nav_items['Nummer'].tolist():
        df_temp = df_nav_items[df_nav_items['Nummer'] == item_no]
        return df_temp[field].iloc[0]
    else:
        return None

# Get info from vendor table in Navision
query_nav_vendor = """  SELECT [No_] AS [Nummer],[Name] AS [Navn]
                  FROM [dbo].[BKI foods a_s$Vendor] """
df_nav_vendor = pd.read_sql(query_nav_vendor, con_nav)

def get_nav_vendor_info(no: str, field: str) -> str:
    """Returns information for the requested vendor and field from Navision. """
    if no in df_nav_vendor['Nummer'].tolist():
        df_temp = df_nav_vendor[df_nav_vendor['Nummer'] == no]
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

# Get item number for requested order number
def get_nav_order_info(order_no: str) -> str:
    """Returns item number for the requested production order based on Navision."""
    if order_no in df_nav_order_info['Ordrenummer'].tolist():
        df_temp = df_nav_order_info[df_nav_order_info['Ordrenummer'] == order_no]
        return df_temp['Varenummer'].iloc[0]
    else:
        return None

# Get dataframe with orders related to input orders from Navision
def get_nav_orders_from_related_orders(orders: str):
    """Returns a Pandas Dataframe with a list of orders related to input orders.
       Returned dataframe is based on Navision -> Reserved production orders."""
    query = f""" SELECT [Prod_ Order No_] AS [Ordrenummer]
                               ,[Reserved Prod_ Order No_] AS [Relateret ordre]
                               ,'Navision reservationer' AS [Kilde]
                               FROM [dbo].[BKI foods a_s$Reserved Prod_ Order No_]
                               WHERE [Reserved Prod_ Order No_] IN 
                               ({orders}) AND [Invalid] = 0 """
    return pd.read_sql(query, con_nav)

# Get information whether a contract/delivery has been tasted and approved.
def get_contract_delivery_approval_id(contract: str, delivery: str) -> str:
    """ Returns the first ID from BKI_Datastore on which a given contract/delivery
        has been approved on. \n Returns None if no approved ID can be found. 
        If None is input as delivery the first approval of contract is returned,
        regardless of any deliveries.
        Only 'modtagelseskontrol' is included in results. """
    query_del = f""" SELECT	MIN(S.[Id]) AS [Id]
                FROM [cof].[Risteri_råkaffe_planlægning] AS RRP
                INNER JOIN [cof].[Risteri_modtagelse_registrering] AS RMR
                	ON RRP.[Id] = RMR.[Id_org]
                	AND RMR.[Id_org_kildenummer] = 3
                INNER JOIN [cof].[Smageskema] AS S
                	ON RMR.[Id] = S.[Id_org]
                	AND S.[Id_org_kildenummer] = 1
                WHERE RRP.[Kontraktnummer] = '{contract}'
                	AND RRP.[Delivery] = '{delivery}'
                	AND S.[Status] = 1
                    AND S.[Smagningstype] = 0"""
    
    query_no_del = f""" SELECT MAX([Id]) AS [Id] FROM [cof].[Smageskema]
                        WHERE [Kontraktnummer] = '{contract}' AND [Smagningstype] = 0
                        AND [Status] = 1 """
    if delivery is None:
        df = pd.read_sql(query_no_del, con_ds)
    else:
        df = pd.read_sql(query_del, con_ds)
    if len(df) == 0:
        return None
    else:
        return str(df['Id'].iloc[0])

# Get subject for emails depending on request type
def get_email_subject(request_reference: str, request_type: int) -> str:
    """Returns a string with subject for email."""
    dict_email_subject = {
        0: f'Anmodet rapport for ordre {request_reference}'
        ,1: f'Anmodet rapport for parti {request_reference}'
        ,2: 'Anmodet rapport for rework'
        ,3: f'Anmodet rapport for handelsvare {request_reference}'
        ,4: f'Anmodet rapport for folie {request_reference}'
        ,5: f'Anmodet rapport for karton {request_reference}'
        ,6: f'Anmodet rapport for ventil {request_reference}'
        ,7: f'Anmodet rapport for risteordre {request_reference}'
    }
    return str(dict_email_subject[request_type])

# Create an empty image. Quick and dirty fix to prevent errors when sending email with report
def create_image_from_binary_string(complete_path: str):
    """
    Creates an empty .png image. This is a quick-and-dirty work arround to prevent
    Power automate from erroring out when trying to send a report without an image attached.
    Parameters
    ----------
    complete_path : str
        Complete path and name for placeholder image.
    """
    with open(complete_path, 'wb') as f:
        f.write('axs123naxmq')

