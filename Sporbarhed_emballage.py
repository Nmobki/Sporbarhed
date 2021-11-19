#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import networkx as nx
import Sporbarhed_shared_functions as ssf


def initiate_report(initiate_id):
    # =============================================================================
    # Variables for query connections
    # =============================================================================
    con_ds = ssf.get_connection('bki_datastore')
    cursor_ds = ssf.get_cursor('bki_datastore')
    engine_ds = ssf.get_engine('bki_datastore')
    con_nav = ssf.get_connection('navision')

    # =============================================================================
    # Read data from request
    # =============================================================================
    query_ds_request =  f""" SELECT TOP 1 [Id] ,[Forespørgselstype],[Rapport_modtager]
                        ,[Referencenummer] ,[Note_forespørgsel] ,[Modtagelse]  ,[Referencetype]
                        FROM [trc].[Sporbarhed_forespørgsel]
                        WHERE [Id] = {initiate_id} """
    df_request = pd.read_sql(query_ds_request, con_ds)

    # Exit script if no request data is found
    ssf.get_exit_check(len(df_request))

    # =============================================================================
    # Set request variables
    # =============================================================================
    req_type = df_request.loc[0, 'Forespørgselstype']
    req_reference_no = df_request.loc[0, 'Referencenummer'].rstrip(' ')
    req_reference_type = df_request.loc[0, 'Referencetype']
    req_recipients = df_request.loc[0, 'Rapport_modtager']
    req_note = df_request.loc[0, 'Note_forespørgsel']
    req_id = df_request.loc[0, 'Id']
    req_roll = df_request.loc[0, 'Modtagelse']

    script_name = 'Sporbarhed_emballage.py'
    # Read setup for section for reporttypes. NAV querys with NOLOCK to prevent deadlocks
    df_sections = ssf.get_ds_reporttype(req_type)

    # =============================================================================
    # Update request that it is initiated and write into log
    # =============================================================================
    cursor_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                      SET [Forespørgsel_igangsat] = getdate()
                      WHERE [Id] = {req_id} """)
    cursor_ds.commit()
    ssf.log_insert(script_name, f'Request id: {req_id} initiated')

    # =============================================================================
    # Variables for files generated
    # =============================================================================
    filepath = ssf.get_filepath('report')
    file_name = f'Rapport_{req_reference_no}_{req_id}'

    wb_name = f'{file_name}.xlsx'
    path_file_wb = filepath + r'\\' + wb_name
    excel_writer = pd.ExcelWriter(path_file_wb, engine='xlsxwriter')

    png_relations_name = f'{file_name}.png'
    path_png_relations = filepath + r'\\' + png_relations_name

    # =============================================================================
    # Queries for each different type of referencetype and requesttype     
    # Queries are defined individually, but only used depending on reference and request type.
    # =============================================================================
    # Query Navision for information with refencetype = 4 (lot no) and no roll number. 
    query_nav_lot = f"""SELECT POAC.[Prod_ Order No_] AS [Produktionsordrenummer]
                    	,POC.[Item No_] AS [Varenummer], [Roll No_] AS [Rullenummer]
                        FROM [dbo].[BKI foods a_s$Prod_ Order Add_ Comp_] AS POAC
                        INNER JOIN [dbo].[BKI foods a_s$Prod_ Order Component] AS POC
                        	ON POAC.[Prod_ Order No_] = POC.[Prod_ Order No_]
                        	AND POAC.[Prod_ Order Line No_] = POC.[Prod_ Order Line No_]
                        	AND POAC.[Prod_ Order Component Line No_] = POC.[Line No_]
                        WHERE POAC.[Batch_Lot No_] = '{req_reference_no}'
                        GROUP BY POAC.[Prod_ Order No_],POC.[Item No_] """
    # Query Navision for information with referencetype = 4 (lotno) and specified roll number.
    query_nav_lot_roll = f"""SELECT POAC.[Prod_ Order No_] AS [Produktionsordrenummer]
                        	,POC.[Item No_] AS [Varenummer], [Roll No_] AS [Rullenummer]	
                            FROM [dbo].[BKI foods a_s$Prod_ Order Add_ Comp_] AS POAC
                            INNER JOIN [dbo].[BKI foods a_s$Prod_ Order Component] AS POC
                            	ON POAC.[Prod_ Order No_] = POC.[Prod_ Order No_]
                            	AND POAC.[Prod_ Order Line No_] = POC.[Prod_ Order Line No_]
                            	AND POAC.[Prod_ Order Component Line No_] = POC.[Line No_]
                            WHERE POAC.[Batch_Lot No_] = '{req_reference_no}'
                            	AND [Roll No_] = '{req_roll}'
                            GROUP BY POAC.[Prod_ Order No_],POC.[Item No_] """
    # Query Navision for information with refencetype = 5 (purchase order) and no roll number. 
    query_nav_purch = f"""SELECT POAC.[Prod_ Order No_] AS [Produktionsordrenummer]
                    	,POC.[Item No_] AS [Varenummer], [Roll No_] AS [Rullenummer]
                        FROM [dbo].[BKI foods a_s$Prod_ Order Add_ Comp_] AS POAC
                        INNER JOIN [dbo].[BKI foods a_s$Prod_ Order Component] AS POC
                        	ON POAC.[Prod_ Order No_] = POC.[Prod_ Order No_]
                        	AND POAC.[Prod_ Order Line No_] = POC.[Prod_ Order Line No_]
                        	AND POAC.[Prod_ Order Component Line No_] = POC.[Line No_]
                        WHERE POAC.[Batch_Lot No_] = '{req_reference_no}'
                        GROUP BY POAC.[Prod_ Order No_],POC.[Item No_] """
    # Query Navision for information with referencetype = 5 (purchase order) and specified roll number.
    query_nav_purch_roll = f"""SELECT POAC.[Prod_ Order No_] AS [Produktionsordrenummer]
                        	,POC.[Item No_] AS [Varenummer], [Roll No_] AS [Rullenummer]	
                            FROM [dbo].[BKI foods a_s$Prod_ Order Add_ Comp_] AS POAC
                            INNER JOIN [dbo].[BKI foods a_s$Prod_ Order Component] AS POC
                            	ON POAC.[Prod_ Order No_] = POC.[Prod_ Order No_]
                            	AND POAC.[Prod_ Order Line No_] = POC.[Prod_ Order Line No_]
                            	AND POAC.[Prod_ Order Component Line No_] = POC.[Line No_]
                            WHERE POAC.[Batch_Lot No_] = '{req_reference_no}'
                            	AND [Roll No_] = '{req_roll}'
                            GROUP BY POAC.[Prod_ Order No_],POC.[Item No_] """
    # Query BKI_Datastore for information with referencetype = 4 (lot no), only for ventil components
    query_ds_lot_ventil = f"""SELECT [Ordrenummer] AS [Produktionsordrenummer], [Varenummer]
                              FROM [cof].[Ventil_registrering]
                              WHERE [Batchnr_stregkode] = '{req_reference_no}'
                              GROUP BY [Varenummer], [Ordrenummer]"""
    # Read any of the above queries into a dataframe, depending on reference type, request type and whether any roll number has been entered.
    if req_type == 4: # Folie
        if req_reference_type == 4: # Lotnumber
            if not req_roll: # req_roll holds a value
                df_generelt = pd.read_sql(query_nav_lot_roll, con_nav)
            else: # req_roll doesn't hold a value
                df_generelt = pd.read_sql(query_nav_lot, con_nav)
        if req_reference_type == 5: # Purchase order
            if not req_roll: # req_roll holds a value
                df_generelt = pd.read_sql(query_nav_purch_roll, con_nav)
            else: # req_roll doesn't hold a value
                df_generelt = pd.read_sql(query_nav_purch, con_nav)
    elif req_type == 5: # Karton
        df_generelt = pd.read_sql(query_nav_lot, con_nav)
    elif req_type == 6: # Ventil
        df_generelt = pd.read_sql(query_ds_lot_ventil, con_nav)
    else:
        df_generelt = pd.DataFrame()
        
    print(df_generelt)


    # =============================================================================
    # Section 1: Generelt
    # =============================================================================
    section_id = 1
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Silo', 'Anmodet dato', 'Sidste tommelding'
                    ,'Efterfølgende tommelding', 'Reworktype']

    df_generelt = df_request
    if ssf.get_section_status_code(df_generelt) == 99:
        try:
            df_generelt['Silo'] = req_reference_no
            df_generelt['Anmodet dato'] = silo_req_date_dmy
            df_generelt['Sidste tommelding'] = ssf.convert_date_format(silo_last_empty_ymd, 'yyyy-mm-dd', 'dd-mm-yyyy')
            df_generelt['Efterfølgende tommelding'] = ssf.convert_date_format(silo_next_empty_ymd, 'yyyy-mm-dd', 'dd-mm-yyyy')
            df_generelt['Reworktype'] = ssf.rework.get_rework_type(req_reference_no)
            # Transpose dataframe
            df_generelt = df_generelt[column_order].transpose()
            df_generelt = df_generelt.reset_index()
            df_generelt.columns = ['Sektion','Værdi']
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_generelt, section_name, True)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e:
            # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else:
        # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_generelt))