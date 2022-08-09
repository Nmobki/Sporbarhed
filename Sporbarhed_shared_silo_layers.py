#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import Sporbarhed_shared_server_information as sssi

# Get silo layers
def get_silo_layer(silo: str, datetime: str, contract: str, delivery: str, return_type: str, return_value: str) ->str:
    """
    Returns a single row dataframe with contract no and delivery name for the last delivery to have been
    in the requested silo before the given date.
    \n Parameters
    ----------
    silo : str
        Silo number.
    datetime : str
        Datetime value formated as string. Can be input as either yyyy-mm-dd or yyyy-mm-dd hh:mm
    contract : str
        Contract number from Probat
    delivery : str
        Delivery number from Probat
    return_type : int
        'next' = return next delivery in silo
        'previous' = return previous delivery in silo
    return_value : str
        Specify which value is to be returned:
        Efterfølgende kontraktnummer | Efterfølgende modtagelse | Foregående kontraktnummer | Foregående modtagelse
    \n Returns
    -------
    String value for 
    """
    query_next = f"""SELECT TOP 1 [CONTRACT_NO] AS [Efterfølgende kontraktnummer]
                        ,[DELIVERY_NAME] AS [Efterfølgende modtagelse]
                        FROM [BKI_IMP_EXP].[dbo].[PRO_EXP_REC_SUM_DEST]
                        WHERE [DESTINATION] = '{silo}'
                        	AND ( [CONTRACT_NO] + [DELIVERY_NAME] <> '{contract}' + '{delivery}')
                        	AND [RECORDING_DATE] > '{datetime}'
                        ORDER BY [RECORDING_DATE] ASC"""
    query_previous = f"""SELECT TOP 1 [CONTRACT_NO] AS [Foregående kontraktnummer]
                        ,[DELIVERY_NAME] AS [Foregående modtagelse]
                        FROM [BKI_IMP_EXP].[dbo].[PRO_EXP_REC_SUM_DEST]
                        WHERE [DESTINATION] = '{silo}'
                        	AND ( [CONTRACT_NO] + [DELIVERY_NAME] <> '{contract}' + '{delivery}')
                        	AND [RECORDING_DATE] < '{datetime}'
                        ORDER BY [RECORDING_DATE] DESC"""
    
    if return_type == 'next':
        df = pd.read_sql(query_next, sssi.con_probat)
        if len(df) == 0:
            return ''
        else:
            return df[return_value]
    elif return_type == 'previous':
        df = pd.read_sql(query_previous, sssi.con_probat)
        if len(df) == 0:
            return ''
        else:
            return df[return_value]
    else:
        raise ValueError('Incorrect return_type specified')

# Get 200-silo layers from input string of order numbers
def get_200silo_layers_from_orders(orders: str):
    """
    Get content of each used 200-silo before and after the use for each order.
    \n Parameters
    ----------
    orders : str
        Input string containing one or more ordernumbers.
        Input string must be prepared for use in SQL IN clause.
    \n Returns
    -------
    Pandas DataFrame.
    """
    query_orders = f"""SELECT [SOURCE] AS [Silo],[S_CONTRACT_NO] AS [Kontraktnummer]
                ,[S_DELIVERY_NAME] AS [Modtagelse],[ORDER_NAME] AS [Produktionsordre],
                CONVERT(NVARCHAR,[RECORDING_DATE], 20 ) AS [LR_DATO]
                FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                WHERE [ORDER_NAME] IN ({orders})"""
    df = pd.read_sql(query_orders, sssi.con_probat)
    df['Efterfølgende kontraktnummer'] = df.apply(lambda x: get_silo_layer(
                                            x.Silo, 
                                            x.LR_DATO, 
                                            x.Kontraktnummer, 
                                            x.Modtagelse, 
                                            'next', 
                                            'Efterfølgende kontraktnummer'), axis=1)
    df['Efterfølgende modtagelse'] = df.apply(lambda x: get_silo_layer(
                                            x.Silo, 
                                            x.LR_DATO, 
                                            x.Kontraktnummer, 
                                            x.Modtagelse, 
                                            'next', 
                                            'Efterfølgende modtagelse'), axis=1)
    df['Foregående kontraktnummer'] = df.apply(lambda x: get_silo_layer(
                                            x.Silo, 
                                            x.LR_DATO, 
                                            x.Kontraktnummer, 
                                            x.Modtagelse, 
                                            'previous', 
                                            'Foregående kontraktnummer'), axis=1)
    df['Foregående modtagelse'] = df.apply(lambda x: get_silo_layer(
                                            x.Silo, 
                                            x.LR_DATO, 
                                            x.Kontraktnummer, 
                                            x.Modtagelse, 
                                            'previous', 
                                            'Foregående modtagelse'), axis=1)
    df = df[['Produktionsordre','Silo','Kontraktnummer','Modtagelse','Foregående kontraktnummer',
            'Foregående modtagelse','Efterfølgende kontraktnummer','Efterfølgende modtagelse']]
    df.drop_duplicates(ignore_index = True, inplace = True)
    return df

