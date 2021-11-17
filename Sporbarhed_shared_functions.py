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

def get_nav_item_info(item_no: str, field: str) -> str:
    """Returns information for the requested item number and field from Navision. """
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

# Get item number for requested order number
def get_nav_order_info(order_no: str) -> str:
    """Returns item number for the requested production order based on Navision."""
    if order_no in df_nav_order_info['Ordrenummer'].tolist():
        df_temp = df_nav_order_info[df_nav_order_info['Ordrenummer'] == order_no]
        return df_temp['Varenummer'].iloc[0]
    else:
        return None

# Get dataframe with
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

# Get subject for emails depending on request type
def get_email_subject(request_reference: str, request_type: int) -> str:
    """Returns a string with subject for email."""
    dict_email_subject = {
        0: f'Anmodet rapport for ordre {request_reference}'
        ,1: f'Anmodet rapport for parti {request_reference}'
        ,2: 'Anmodet rapport for opspræt'
        ,3: f'Anmodet rapport for handelsvare {request_reference}'
    }
    return str(dict_email_subject[request_type])


# =============================================================================
# Functions related to rework in seperate class only for organizational purposes
# =============================================================================

class rework():
    # Get last empty signal from a given silo before requested date
    def get_silo_last_empty(silo: str, date: str):
        """Returns date containing date for last emptying of the requested silo.
           Date is to be formated 'yyyy-mm-dd'.
           If no date is found None is returned."""
        query = f""" SELECT	MAX(DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)) AS [Dato]
                     FROM [dbo].[PRO_EXP_SILO_DIF]
                     WHERE [SILO] = '{silo}'
                     AND DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) < '{date}' """
        df = pd.read_sql(query, con_probat)
        if len(df) == 0 or df['Dato'].iloc[0] is None:
            return None
        else:
            df['Dato'] = df['Dato'].apply(lambda x: x.strftime('%Y-%m-%d'))
            return str(df['Dato'].iloc[0])

    # Get the first empty signal from a given silo after the requested date
    def get_silo_next_empty(silo: str, date: str):
        """Returns date containing date for next emptying of the requested silo.
           Date is to be formated 'yyyy-mm-dd'.
           If no date is found None is returned."""
        query = f""" SELECT	MIN(DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)) AS [Dato]
                     FROM [dbo].[PRO_EXP_SILO_DIF]
                     WHERE [SILO] = '{silo}'
                     AND DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) > '{date}' """
        df = pd.read_sql(query, con_probat)
        if len(df) == 0 or df['Dato'].iloc[0] is None:
            return None
        else:
            df['Dato'] = df['Dato'].apply(lambda x: x.strftime('%Y-%m-%d'))
        return str(df['Dato'].iloc[0])

    # Get the type of rework from the silo
    def get_rework_type(silo: str) -> str:
        "Returns type of rework based on input silo."
        if silo in ['401','403']:
            rework_type = 'Helbønne'
        elif silo in ['511','512']:
            rework_type = 'Formalet'
        return rework_type

    # Get grinding orders that have used rework from the requested silos between relevant dates
    def get_rework_orders_from_dates(silo: str, start_date: str, end_date: str):
        """
        Get dataframe containing all grinding orders which have used rework from the specified
        silo between the two input dates.
        Data is fetched from Probat.
        \n Parameters
        ----------
        silo : str
            Silo from which rework has been used.
        start_date : str
            Start date for the period which is to be queried for.
        end_date : str
            End date for the period which is to be queried for.
            If no end date is supplied todays date is used instead.
        \n Returns
        -------
        df : Pandas dataframe
            Returns a pandas dataframe with date and order numbers.
        """
        if None in (silo, start_date):
            return None
        # Set end_date to todays date if input end_date is None
        if end_date in ('', None):
            end_date = datetime.now().strftime('%Y-%m-%d')
        query = f""" WITH [ORDERS_CTE] AS (
                SELECT DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                    ,[SOURCE] AS [Silo],[ORDER_NAME] AS [Ordrenummer]
                FROM [dbo].[PRO_EXP_ORDER_LOAD_G]
                WHERE [ORDER_NAME] IS NOT NULL
                GROUP BY DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                    ,[ORDER_NAME], [SOURCE]
                UNION ALL
                SELECT DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                    ,[DEST_NAME],[ORDER_NAME]
                FROM [dbo].[PRO_EXP_ORDER_UNLOAD_G]
                WHERE [ORDER_NAME] IS NOT NULL
                GROUP BY DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                    ,[ORDER_NAME], [DEST_NAME] )
                SELECT [Dato], [Ordrenummer] FROM [ORDERS_CTE]
                WHERE [Silo] = '{silo}' AND [Dato] BETWEEN '{start_date}' AND '{end_date}' 
                GROUP BY  [Dato], [Ordrenummer] """
        df = pd.read_sql(query, con_probat)
        return df

    # Get a dataframe containing all orders which have used rework silos as well as use dates
    def get_rework_silos(orders_string: str):
        """Returns a pandas dataframe with all grinding orders that have used rework.
           Returned dataframe is based on input string of order numbers, and only orders
           which have used rework will be included in the returned dataframe.
           Data is fetched from Probat."""
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
    def get_rework_prøvesmagning(start_date: str, end_date: str, silo: str, order_no: str):
        """
        Get dataframe with all registrations regarding rework from prøvesmagning.
        Data is fetched from BKI_Datastore cof.rework_registrering and cof.Rework_prøvesmagning
        \n Parameters
        ----------
        start_date : str
            Start date formated as 'yyyy-mm-dd'. Used to defined start of period for which data is searched.
        end_date : str
            End date formated as 'yyyy-mm-dd'. Used to defined end of period for which data is searched.
        silo : str
            Silo which the rework must have been added to.
        order_no : str
            Order_number which the request is based on.
        \n Returns
        -------
        df_temp : Pandas Dataframe
            Dataframe containing productionorders, silo, source and requested order number.
        """
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
        """
        Get dataframe with all registrations regarding rework from pakkeri.
        Data is fetched from BKI_Datastore cof.rework_registrering which is then used to filter final query from Navision Item Ledger Entry.
        \n Parameters
        ----------
        start_date : str
            Start date formated as 'yyyy-mm-dd'. Used to defined start of period for which data is searched.
        end_date : str
            End date formated as 'yyyy-mm-dd'. Used to defined end of period for which data is searched.
        silo : str
            Silo which the rework must have been added to.
        order_no : str
            Order_number which the request is based on.
        \n Returns
        -------
        df_temp : Pandas Dataframe
            Dataframe containing productionorders, silo, source and requested order number.
        """
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
        """
        Get dataframe with all registrations regarding rework from komprimatorrum.
        Data is fetched from BKI_Datastore cof.rework_registrering and cof.Rework_prøvesmagning
        \n Parameters
        ----------
        start_date : str
            Start date formated as 'yyyy-mm-dd'. Used to defined start of period for which data is searched.
        end_date : str
            End date formated as 'yyyy-mm-dd'. Used to defined end of period for which data is searched.
        silo : str
            Silo which the rework must have been added to.
        order_no : str
            Order_number which the request is based on.
        \n Returns
        -------
        df_temp : Pandas Dataframe
            Dataframe containing productionorders, silo, source and requested order number.
        """
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
        """
        Get dataframe with all registrations regarding rework from henstandsprøver.
        Data is fetched from BKI_Datastore cof.rework_registrering which is then used to filter final query from Navision Item Ledger Entry.
        \n Parameters
        ----------
        start_date : str
            Start date formated as 'yyyy-mm-dd'. Used to defined start of period for which data is searched.
        end_date : str
            End date formated as 'yyyy-mm-dd'. Used to defined end of period for which data is searched.
        silo : str
            Silo which the rework must have been added to.
        order_no : str
            Order_number which the request is based on.
        \n Returns
        -------
        df_temp : Pandas Dataframe
            Dataframe containing productionorders, silo, source and requested order number.
        """
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
        """
        This accepts a Pandas Dataframe as input parameter.
        This is used to call the following functions:
            get_rework_prøvesmagning, get_rework_pakkeri, get_rework_komprimatorrum, get_rework_henstandsprøver
        \n Parameters
        ----------
        df_silos : Pandas Dataframe
            Dataframe must contain columns named: 'Startdato','Slutdato','Silo','Produktionsordre'.
        \n Returns
        -------
        Pandas Dataframe['Produktionsordre','Silo','Indhold','Kilde']
            Returns a Pandas Dataframe containing all identified components of rework possibly used in the orders contained in input dataframe.
        """
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
        if len(df_rework) == 0:
            return pd.DataFrame()
        else:
            return df_rework[['Produktionsordre','Silo','Indhold','Kilde']]

class finished_goods():
    # Recursive query to get lotnumbers related to any of the input orders.
    def get_nav_lotnos_from_orders(orders_string: str, return_type: str):
        """
        Returns all relevant lotnumbers from Navision Item Ledger Entry that are related to input orders.
        \n Parameters
        ----------
        orders_string : str
            String ready for SQL querying containing all order numbers which are the base for identifying all relevant lotnumbers.
        return_type : str
            'dataframe' or 'string'.
            Defined which type of data is returned from the function.

        \n Returns
        -------
        Pandas Dataframe or a string
            Depending on input parameter "return_type" either a dataframe or a string is returned with all identified lotnumbers..
        """
        query = f""" WITH [LOT_ORG] AS ( SELECT [Lot No_]
                                  FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK)
                                  WHERE [Order No_] IN ({orders_string})
                                  AND [Entry Type] = 6
                                  UNION ALL
                                  SELECT ILE_O.[Lot No_]
                                  FROM [LOT_ORG]
                                  INNER JOIN [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) AS ILE_C
                                      ON [LOT_ORG].[Lot No_] = ILE_C.[Lot No_]
                                      AND [ILE_C].[Entry Type] IN (5,8)
                                  INNER JOIN [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) AS ILE_O
                                	  ON ILE_C.[Document No_] = ILE_O.[Document No_]
                                      AND ILE_O.[Entry Type] IN (6,9)
                                  INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
    								  ON ILE_O.[Item No_] = I.[No_]
								  WHERE I.[Item Category Code] = 'FÆR KAFFE')
                                  SELECT [Lot No_] AS [Lot]
                                  FROM [LOT_ORG] GROUP BY [Lot No_] """
        df = pd.read_sql(query, con_nav)
        if return_type == 'dataframe':
            return df
        elif return_type == 'string':
            return string_to_sql(df['Lot'].unique().tolist())

    # Get information per Order no. based on string of requested lotnumbers.
    def get_production_information(lotnumbers: str):
        """
        Get information about the production of any amount of lotnumbers.
        Data is from Navision Item Ledger Entry
        \n Parameters
        ----------
        lotnumbers : str
            String ready for SQL querying containing any amount of lotnumbers..
        \n Returns
        -------
        Pandas Dataframe
        """
        query = f""" WITH [LOT_SINGLE] AS ( SELECT [Lot No_], [Document No_] AS [Ordrenummer]
                                  FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) 
    							  WHERE [Entry Type] IN (6,9)
    							  GROUP BY [Lot No_], [Document No_])
                                  SELECT ILE.[Item No_] AS [Varenummer],I.[Description] AS [Varenavn], LOT_SINGLE.[Ordrenummer]
                            	  ,SUM(CASE WHEN ILE.[Entry Type] IN (0,6,9)
                            		THEN ILE.[Quantity] * I.[Net Weight]
                            		ELSE 0 END) AS [Produceret]
                            	,SUM(CASE WHEN ILE.[Entry Type] = 1
                            		THEN ILE.[Quantity] * I.[Net Weight] * -1
                            		ELSE 0 END) AS [Salg]
                            	,SUM(CASE WHEN ILE.[Entry Type] NOT IN (0,1,6,9)
                            		THEN ILE.[Quantity] * I.[Net Weight] * -1
                            		ELSE 0 END) AS [Regulering & ompak]
                            	,SUM(ILE.[Remaining Quantity] * I.[Net Weight]) AS [Restlager]
                                FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) AS ILE
                                INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
                                	ON ILE.[Item No_] = I.[No_]
                                INNER JOIN [LOT_SINGLE]
                                	ON ILE.[Lot No_] = [LOT_SINGLE].[Lot No_]
    							WHERE ILE.[Lot No_] IN ({lotnumbers})
                                GROUP BY ILE.[Item No_],I.[Description], LOT_SINGLE.[Ordrenummer] """
        return pd.read_sql(query, con_nav)

    # Get information about any sales to any customers based on input list of lotnumbers.
    def get_sales_information(lotnumbers: str):
        """
        Get information about the sales of any amount of lotnumbers.
        Data is from Navision Item Ledger Entry
        \n Parameters
        ----------
        lotnumbers : str
            String ready for SQL querying containing any amount of lotnumbers..
        \n Returns
        -------
        Pandas Dataframe
        """
        query = f""" WITH [LOT_SINGLE] AS ( SELECT [Lot No_], [Document No_] AS [Produktionsordrenummer]
                          FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK)
    					  WHERE [Entry Type] IN (6,9) 
    					  GROUP BY [Lot No_],[Document No_] )
    
                          SELECT C.[No_] AS [Debitornummer],C.[Name] AS [Debitornavn], LOT_SINGLE.[Produktionsordrenummer]
                        	  ,ILE.[Posting Date] AS [Dato]
                        	  ,ILE.[Item No_] AS [Varenummer]
                        	  ,SUM(ILE.[Quantity] * -1) AS [Enheder]
                        	  ,SUM(ILE.[Quantity] * I.[Net Weight] * -1) AS [Kilo]
                          FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) AS ILE
                          INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
                        	  ON ILE.[Item No_] = I.[No_]
                          INNER JOIN [LOT_SINGLE]
                          	  ON ILE.[Lot No_] = [LOT_SINGLE].[Lot No_]
                          INNER JOIN [dbo].[BKI foods a_s$Customer] (NOLOCK) AS C
                        	  ON ILE.[Source No_] = C.[No_]
                          WHERE ILE.[Entry Type] = 1
    						AND ILE.[Lot No_] IN ({lotnumbers})
                          GROUP BY  C.[No_] ,C.[Name],ILE.[Posting Date],ILE.[Item No_], LOT_SINGLE.[Produktionsordrenummer] """
        return pd.read_sql(query, con_nav)

    def get_order_relationship(lotnumbers: str):
        """
        Returns a dataframe containing information about any order and all related orders if the primary order has been used to created display items.
        Data is from Navision Item Ledger Entry
        \n Parameters
        ----------
        lotnumbers : str
            String ready for SQL querying containing any amount of lotnumbers..
        \n Returns
        -------
        Pandas Dataframe
            Returns a pandas dataframe containing order numbers for the primary and secondary orders..

        """
        query = f""" WITH [DOC_CONS] AS ( SELECT [Lot No_], [Document No_]
                                  FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK)
                                  WHERE [Entry Type] IN (5,8)
                                  GROUP BY [Lot No_], [Document No_] )
                                  ,[DOC_OUT] AS ( SELECT [Lot No_], [Document No_]
                                  FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK)
                                  WHERE [Entry Type] IN (6,9)
                                  GROUP BY [Lot No_], [Document No_] )
                                  SELECT DO.[Document No_] AS [Relateret ordre]
                                  ,DC.[Document No_] AS [Ordrenummer]
                                  ,'Navision forbrug' AS [Kilde]
                                  FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) AS ILE
                                  INNER JOIN [DOC_OUT] AS DO
                                      ON ILE.[Lot No_] = DO.[Lot No_]
                                  LEFT JOIN [DOC_CONS] AS DC
                                      ON ILE.[Lot No_] = DC.[Lot No_]
                                  WHERE DC.[Document No_] IS NOT NULL
    							  AND ILE.[Lot No_] IN ({lotnumbers})
                                  GROUP BY DO.[Document No_] ,DC.[Document No_] """
        return pd.read_sql(query, con_nav)
        