#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import networkx as nx
import Sporbarhed_shared_functions as ssf
import Sporbarhed_shared_rework as ssr
import Sporbarhed_shared_finished_goods as ssfg
import Sporbarhed_shared_silo_layers as sssl


def initiate_report(initiate_id):

    # =============================================================================
    # Variables for query connections
    # =============================================================================
    con_ds = ssf.get_connection('bki_datastore')
    cursor_ds = ssf.get_cursor('bki_datastore')
    engine_ds = ssf.get_engine('bki_datastore')
    con_nav = ssf.get_connection('navision')
    con_probat = ssf.get_connection('probat')

    # =============================================================================
    # Read data from request
    # =============================================================================
    query_ds_request =  f""" SELECT TOP 1 [Id] ,[Forespørgselstype],[Rapport_modtager]
                        ,[Referencenummer] ,[Note_forespørgsel] ,[Modtagelse]  ,[Ordrerelationstype]
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
    req_recipients = df_request.loc[0, 'Rapport_modtager']
    req_note = df_request.loc[0, 'Note_forespørgsel']
    req_id = df_request.loc[0, 'Id']
    req_ordrelationstype = df_request.loc[0, 'Ordrerelationstype']

    script_name = 'Sporbarhed_risteordre.py'
    orders_top_level = [req_reference_no]
    orders_related = []
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
    # Queries for different parts of report
    # =============================================================================
    # Query to read various information from BKI_Datastore for the order requested in the report
    query_ds_generelt = f""" WITH [KP] AS ( SELECT [Ordrenummer]
                    	,SUM( CASE WHEN [Prøvetype] = 0 THEN [Antal_prøver] ELSE 0 END) AS [Kontrolprøve]
                    	,SUM( CASE WHEN [Prøvetype] = 1 THEN [Antal_prøver] ELSE 0 END) AS [Referenceprøve]
                    	,SUM( CASE WHEN [Prøvetype] = 2 THEN [Antal_prøver] ELSE 0 END) AS [Henstandsprøve]
                        FROM [cof].[Kontrolskema_prøver]
                        GROUP BY [Ordrenummer] )
                        ,[SK] AS ( SELECT [Referencenummer] ,MAX([Status]) AS [Status]
                        FROM [cof].[Smageskema] WHERE [Referencetype] = 2
                        GROUP BY [Referencenummer] )
                        SELECT SF.[Referencenummer] AS [Ordrenummer] ,SF.[Pakketidspunkt] ,KH.[Igangsat_af] AS [Igangsat af]
                        ,KH.[Silo_opstart] AS [Opstartssilo] ,KH.[Taravægt] ,KH.[Nitrogen] / 100.0 AS [Nitrogen]
                        ,KH.[Bemærkning] AS [Bemærkning opstart] ,ISNULL(KP.[Kontrolprøve] ,0) AS [Kontrolprøver]
                        ,ISNULL(KP.[Referenceprøve] ,0) AS [Referenceprøver]
                        ,ISNULL(KP.[Henstandsprøve] ,0) AS [Henstandsprøver]
                        ,CASE WHEN SK.[Status] = 1 THEN 'Godkendt' WHEN SK.[Status] = 0 THEN 'Afvist'
                        ELSE 'Ej smagt' END AS [Smagning status], KH.[Pakkelinje]
                        FROM [trc].[Sporbarhed_forespørgsel] AS SF
                        LEFT JOIN [cof].[Kontrolskema_hoved] AS KH 
                            ON SF.[Referencenummer] = KH.[Ordrenummer]
                        LEFT JOIN [KP] 
                            ON SF.[Referencenummer] = KP.[Ordrenummer]
                        LEFT JOIN [SK] 
                            ON SF.[Referencenummer] = SK.[Referencenummer]
                        WHERE SF.[Id] = {req_id} """
    df_results_generelt = pd.read_sql(query_ds_generelt, con_ds)

    # =============================================================================
    # Section 1: Generelt
    # =============================================================================
    section_id = 1
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = []
    columns_1_dec = []

    if ssf.get_section_status_code(df_nav_generelt) == 99:
        try:
            df_nav_generelt['Pakkelinje'] = df_results_generelt['Pakkelinje'].iloc[0]
            df_nav_generelt['Pakketidspunkt'] = df_results_generelt['Pakketidspunkt'].iloc[0]
            df_nav_generelt['Ordrenummer'] = req_reference_no
            df_nav_generelt['Smagning status'] = df_results_generelt['Smagning status'].iloc[0]
            df_nav_generelt['Opstartssilo'] = df_results_generelt['Opstartssilo'].iloc[0]
            df_nav_generelt['Igangsat af'] = df_results_generelt['Igangsat af'].iloc[0]
            df_nav_generelt['Taravægt'] = df_results_generelt['Taravægt'].iloc[0]
            df_nav_generelt['Nitrogen'] = df_results_generelt['Nitrogen'].iloc[0]
            df_nav_generelt['Lotnumre produceret'] = len(df_nav_lotno)
            df_nav_generelt['Henstandsprøver'] = df_results_generelt['Henstandsprøver'].iloc[0]
            df_nav_generelt['Referenceprøver'] = df_results_generelt['Referenceprøver'].iloc[0]
            df_nav_generelt['Kontrolprøver'] = df_results_generelt['Kontrolprøver'].iloc[0]
            df_nav_generelt['Bemærkning opstart'] = df_results_generelt['Bemærkning opstart'].iloc[0]
            # Apply column formating
            for col in columns_1_dec:
                df_nav_generelt[col] = df_nav_generelt[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            df_nav_generelt['Produktionsdato'] = df_nav_generelt['Produktionsdato'].dt.strftime('%d-%m-%Y')
            # Transpose dataframe
            df_nav_generelt = df_nav_generelt[column_order].transpose()
            df_nav_generelt = df_nav_generelt.reset_index()
            df_nav_generelt.columns = ['Sektion','Værdi']
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_nav_generelt, section_name, True)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_nav_generelt))



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
                      ,'Emne': ssf.get_email_subject(req_reference_no, req_type)
                      ,'Forespørgsels_id': req_id
                      ,'Note':req_note}
    pd.DataFrame(data=dict_email_log, index=[0]).to_sql('Sporbarhed_email_log', con=engine_ds, schema='trc', if_exists='append', index=False)
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
