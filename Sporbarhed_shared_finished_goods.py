#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import Sporbarhed_shared_server_information as sssi
import Sporbarhed_shared_functions as ssf


# =============================================================================
# Variables for query connections
# =============================================================================
con_nav = sssi.con_nav

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
        return ssf.string_to_sql(df['Lot'].unique().tolist())

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
							  INNER JOIN [dbo].[BKI foods a_s$Production Order] AS PO
							      ON DC.[Document No_] = PO.[No_]
                              WHERE DC.[Document No_] IS NOT NULL
							  AND PO.[Source No_] NOT IN ('10401401','10401403','10502401','10502403')
							  AND ILE.[Lot No_] IN ({lotnumbers})
                              GROUP BY DO.[Document No_] ,DC.[Document No_],PO.[Source No_]"""
    return pd.read_sql(query, con_nav)
    