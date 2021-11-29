#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import Sporbarhed_shared_server_information as sssi
import Sporbarhed_shared_functions as ssf
import Sporbarhed_shared_rework as ssr
from datetime import datetime


# =============================================================================
# Variables for query connections
# =============================================================================
con_ds = sssi.con_ds
con_nav = sssi.con_nav
con_probat = sssi.con_probat

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
        return '2021-11-01'
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
    if len(df) == 0:
        return pd.DataFrame(columns=['Slutdato','Silo','Produktionsordre','Startdato'])
    df['Temp_date'] = df['Slutdato'].dt.strftime('%Y-%m-%d')
    df['Startdato'] = df.apply(lambda x: get_silo_last_empty(x.Silo, x.Temp_date), axis=1)
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
            # Get nessecary info for filtering
            df_temp['Varenummer'] = df_temp.apply(lambda x: ssf.get_nav_order_info(x.Indhold), axis=1)
            df_temp['Kaffetype'] = df_temp.apply(lambda x: ssf.get_nav_item_info(x.Varenummer, 'Kaffetype'), axis=1)
            df_temp['Kaffetype_silo'] = df_temp.apply(lambda x: ssr.get_rework_type(silo), axis=1)
            # Filter columns
            df_temp.query('Kaffetype_silo == Kaffetype', inplace=True)
            # Add last info and return relevant columns
            df_temp['Silo'] = silo
            df_temp['Produktionsordre'] = order_no
            df_temp['Kilde'] = 'Prøvesmagning'
            return df_temp[['Indhold','Silo','Produktionsordre','Kilde']]

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
            df_prøvesmagning = get_rework_prøvesmagning(startdato, slutdato, silo, ordrenummer)
            df_pakkeri = get_rework_pakkeri(startdato, slutdato, silo, ordrenummer)
            df_komprimatorrum = get_rework_komprimatorrum(startdato, slutdato, silo, ordrenummer)
            df_henstandsprøver = get_rework_henstandsprøver(startdato, slutdato, silo, ordrenummer)
            # Concat each function to one dataframe
            df_rework = pd.concat([df_rework, df_prøvesmagning, df_pakkeri, df_komprimatorrum, df_henstandsprøver])
    if len(df_rework) == 0:
        return pd.DataFrame()
    else:
        return df_rework[['Produktionsordre','Silo','Indhold','Kilde']]

