#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import networkx as nx
import Sporbarhed_shared_functions as ssf
import Sporbarhed_shared_rework as ssr
import Sporbarhed_shared_finished_goods as ssfg
import Sporbarhed_shared_silo_layers as sssl
import Sporbarhed_shared_server_information as sssi


def initiate_report(initiate_id):
    # =============================================================================
    # Read data from request
    # =============================================================================
    query_ds_request =  f""" SELECT TOP 1 [Id] ,[Forespørgselstype],[Rapport_modtager]
                        ,[Referencenummer] ,[Note_forespørgsel] ,[Modtagelse]  ,[Ordrerelationstype]
                        FROM [trc].[Sporbarhed_forespørgsel]
                        WHERE [Id] = {initiate_id} """
    df_request = pd.read_sql(query_ds_request, sssi.con_ds)

    # Exit script if no request data is found
    ssf.get_exit_check(len(df_request))

    # =============================================================================
    # Set request variables
    # =============================================================================
    req_type = df_request.loc[0, 'Forespørgselstype']
    req_order_no = df_request.loc[0, 'Referencenummer']
    req_recipients = df_request.loc[0, 'Rapport_modtager']
    req_note = df_request.loc[0, 'Note_forespørgsel']
    req_id = df_request.loc[0, 'Id']
    req_ordrelationstype = df_request.loc[0, 'Ordrerelationstype']

    script_name = 'Sporbarhed_risteordre.py'
    orders_top_level = [req_order_no]
    orders_related = []
    # Read setup for section for reporttypes. NAV querys with NOLOCK to prevent deadlocks
    df_sections = ssf.get_ds_reporttype(req_type)

    # =============================================================================
    # Update request that it is initiated and write into log
    # =============================================================================
    sssi.con_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                      SET [Forespørgsel_igangsat] = getdate()
                      WHERE [Id] = {req_id} """)
    # ssf.log_insert(script_name, f'Request id: {req_id} initiated')

    # =============================================================================
    # Variables for files generated
    # =============================================================================
    filepath = sssi.report_filepath
    file_name = f'Rapport_{req_order_no}_{req_id}'

    wb_name = f'{file_name}.xlsx'
    path_file_wb = filepath + r'\\' + wb_name
    excel_writer = pd.ExcelWriter(path_file_wb, engine='xlsxwriter')

    png_relations_name = f'{file_name}.png'
    path_png_relations = filepath + r'\\' + png_relations_name

    # =============================================================================
    # Queries for different parts of report
    # =============================================================================
    # Query to read various information from BKI_Datastore for the order requested in the report
    query_generelt = f""" SELECT [S_CUSTOMER_CODE] AS [Receptnummer]
                    	 ,DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                    	 ,[PRODUCTION_ORDER_ID] AS [Probat id] ,[SOURCE_NAME] AS [Rister]
                    	 ,[DEST_NAME] AS [Silo] ,SUM([WEIGHT] / 1000.0) AS [Kilo]
                         FROM [dbo].[PRO_EXP_ORDER_UNLOAD_R]
                         WHERE [ORDER_NAME] = '{req_order_no}'
                         GROUP BY [S_CUSTOMER_CODE] ,DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                    	,[PRODUCTION_ORDER_ID] ,[SOURCE_NAME],[DEST_NAME] """
    df_generelt = pd.read_sql(query_generelt, sssi.con_probat)

    # =============================================================================
    # Section 1: Generelt
    # =============================================================================
    section_id = 1
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Receptnummer', 'Receptnavn', 'Farve sætpunkt', 'Vandprocent sætpunkt',
                    'Dato', 'Rister', 'Probat id', 'Silo', 'Kilo']
    columns_1_dec = ['Kilo']
    columns_2_pct = ['Vandprocent sætpunkt']
    columns_strip = ['Dato','Silo']
    if ssf.get_section_status_code(df_generelt) == 99:
        try:
            df_generelt['Dato'] = df_generelt['Dato'].dt.strftime('%d-%m-%Y')
            # Group columns
            df_generelt = df_generelt.groupby(['Receptnummer','Rister', 'Probat id']).agg(
               {'Dato': lambda x: ','.join(sorted(pd.Series.unique(x))),
                'Silo': lambda x: ','.join(sorted(pd.Series.unique(x))),
                'Kilo': 'sum'
               }).reset_index()
            # Lookup values from item table
            df_generelt['Receptnavn'] = df_generelt.apply(lambda x: ssf.get_nav_item_info(x.Receptnummer, 'Beskrivelse'), axis=1)
            df_generelt['Farve sætpunkt'] = df_generelt.apply(lambda x: ssf.get_nav_item_info(x.Receptnummer, 'Farve'), axis=1)
            df_generelt['Vandprocent sætpunkt'] = df_generelt.apply(lambda x: ssf.get_nav_item_info(x.Receptnummer, 'Vandprocent'), axis=1)
            # Data/column formating
            for col in columns_strip:
                df_generelt[col] = df_generelt[col].apply(lambda x: ssf.strip_comma_from_string(x))            
            for col in columns_1_dec:
                df_generelt[col] = df_generelt[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            for col in columns_2_pct:
                df_generelt[col] = df_generelt[col].apply(lambda x: ssf.number_format(x, 'pct_2'))
            # Transpose dataframe
            df_generelt = df_generelt[column_order].transpose()
            df_generelt = df_generelt.reset_index()
            df_generelt.columns = ['Sektion','Værdi']
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_generelt, section_name, True)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_generelt))

    # =============================================================================
    # Section 25: Probat ristebatch
    # =============================================================================
    section_id = 25
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Batch id', 'Batchnummer', 'Kilo råkaffe', 'Kilo ristet kaffe', 'Kilo svind',
                    'Svind procent', 'Farve ny', 'Farve gammel', 'Vandprocent', 'Liter vand',
                    'Sluttemperatur', 'Ristetid', 'Tid i forvarmer', 'Energiforbrug (kwh)',
                    'Gasforbrug (m3)']
    columns_1_dec = ['Kilo råkaffe', 'Kilo ristet kaffe', 'Kilo svind', 'Energiforbrug (kwh)',
                     'Gasforbrug (m3)', 'Liter vand']
    columns_2_pct = ['Svind procent', 'Vandprocent']

    query_probat_batch = f""" WITH CTE_LR AS (
                            SELECT [BATCH_ID] AS [Batch id],SUM([WEIGHT] / 1000.0) AS [Kilo råkaffe]
                            FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                            WHERE [ORDER_NAME] = '{req_order_no}'
                            GROUP BY [BATCH_ID] )
                            ,CTE_ULR AS (
                            SELECT [BATCH_ID] ,SUM([WEIGHT] / 1000.0) AS [Kilo ristet kaffe]
                            FROM [dbo].[PRO_EXP_ORDER_UNLOAD_R]
                            WHERE [ORDER_NAME] = '{req_order_no}'
                            GROUP BY [BATCH_ID] )
                            ,CTE_SAMPLES AS (
                            SELECT [BATCH_ID] ,[COLOR_NEW] / 100.0 AS [Farve ny]
                            ,[COLOR_OLD] / 100.0 AS [Farve gammel],[HUMIDITY] / 100.0 / 100.0 AS [Vandprocent]
                            FROM [dbo].[PRO_EXP_SAMPLE_ROASTER]
                            WHERE [PRO_EXPORT_GENERAL_ID] IN ( SELECT MAX([PRO_EXPORT_GENERAL_ID]) FROM [dbo].[PRO_EXP_SAMPLE_ROASTER] GROUP BY [BATCH_ID])
                            )
                            
                            SELECT CTE_LR.[Batch id] ,ROW_NUMBER() OVER (ORDER BY CTE_LR.[Batch id]) AS [Batchnummer]
                            	,CTE_LR.[Kilo råkaffe] ,CTE_ULR.[Kilo ristet kaffe]
                            	,CTE_LR.[Kilo råkaffe] - CTE_ULR.[Kilo ristet kaffe] AS [Kilo svind]
                            	,CTE_SAMPLES.[Farve ny] ,CTE_SAMPLES.[Farve gammel]
                            	,CTE_SAMPLES.[Vandprocent] ,BDR.[WATER_PRECOOLING] / 10.0 AS [Liter vand]
                            	,BDR.[FINAL_TEMP_ROASTING] / 10.0 AS [Sluttemperatur]
                            	,BDR.[ROAST_TIME] / 10.0 AS [Ristetid]
                            	,BDR.[TIME_PREHEATING] / 10.0 AS [Tid i forvarmer]
                            	,BDR.[ENERGY_CONSUMPTION] /10.0 AS [Energiforbrug (kwh)]
                            	,( BDR.[FUEL_MAIN_BURNER] + BDR.[FUEL_AFTER_BURNER] ) / 10.0 AS [Gasforbrug (m3)]
                            FROM CTE_LR
                            LEFT JOIN CTE_ULR ON CTE_LR.[Batch id] = CTE_ULR.[BATCH_ID]
                            LEFT JOIN [dbo].[PRO_EXP_BATCH_DATA_ROASTER] AS BDR ON CTE_LR.[Batch id] = BDR.[BATCH_ID]
                            LEFT JOIN CTE_SAMPLES ON CTE_LR.[Batch id] = CTE_SAMPLES.[BATCH_ID] """
    df_probat_batch = pd.read_sql(query_probat_batch, sssi.con_probat)

    if ssf.get_section_status_code(df_probat_batch) == 99:
        try:
            #TODO!!!!!!!!!!!! Add a total with sum of kilos and loss percentage
            
            # Calculate loss percentage
            df_probat_batch['Svind procent'] = df_probat_batch.apply(lambda x: ssf.zero_division(x['Kilo svind'], x['Kilo råkaffe'], 'Zero'), axis=1)
            # Data/column formating         
            for col in columns_1_dec:
                df_probat_batch[col] = df_probat_batch[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            for col in columns_2_pct:
                df_probat_batch[col] = df_probat_batch[col].apply(lambda x: ssf.number_format(x, 'pct_2'))
            df_probat_batch = df_probat_batch[column_order]
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_probat_batch, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_batch))



    # =============================================================================
    # Section 18: Sektionslog
    # =============================================================================
    section_id = 18
    df_section_log = ssf.get_ds_section_log(req_id)
    section_name = ssf.get_section_name(section_id, df_sections)

    if ssf.get_section_status_code(df_section_log) == 99:
        try:
            df_section_log['Registreringstidspunkt'] = df_section_log['Registreringstidspunkt'].dt.strftime('%H:%M%:%S')
            df_section_log.sort_values(by=['Sektionskode'], inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_section_log, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_section_log))

    #Save files
    excel_writer.save()
    # ssf.log_insert(script_name, f'Excel file {file_name} created')

    # =============================================================================
    # Write into email log
    # =============================================================================
    dict_email_log = {'Filsti': filepath
                      ,'Filnavn': file_name
                      ,'Modtager': req_recipients
                      ,'Emne': ssf.get_email_subject(req_order_no, req_type)
                      ,'Forespørgsels_id': req_id
                      ,'Note':req_note}
    pd.DataFrame(data=dict_email_log, index=[0]).to_sql('Sporbarhed_email_log', con=sssi.con_ds, schema='trc', if_exists='append', index=False)
    # ssf.log_insert(script_name, f'Request id: {req_id} inserted into [trc].[Email_log]')

    # =============================================================================
    # Update request that dataprocessing has been completed
    # =============================================================================
    # cursor_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
    #                   SET Data_færdigbehandlet = 1
    #                   WHERE [Id] = {req_id}""")
    # cursor_ds.commit()
    # ssf.log_insert(script_name, f'Request id: {req_id} completed')

    # Exit script
    ssf.get_exit_check(0)
