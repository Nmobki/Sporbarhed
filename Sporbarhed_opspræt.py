#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from datetime import datetime
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
    con_probat = ssf.get_connection('probat')
    
    # =============================================================================
    # Read data from request
    # =============================================================================
    query_ds_request =  f""" SELECT TOP 1 [Id] ,[Forespørgselstype],[Rapport_modtager], [Dato]
                        ,[Referencenummer] ,[Note_forespørgsel] ,[Modtagelse]  ,[Ordrerelationstype]
                        FROM [trc].[Sporbarhed_forespørgsel]
                        WHERE [Id] = {initiate_id} """
    df_request = pd.read_sql(query_ds_request, con_ds)
    
    # Exit script if no request data is found
    if len(df_request) == 0:
        raise SystemExit(0)
    
    # =============================================================================
    # Set request variables
    # =============================================================================
    req_type = df_request.loc[0, 'Forespørgselstype']
    req_reference_no = df_request.loc[0, 'Referencenummer'].rstrip(' ')
    req_recipients = df_request.loc[0, 'Rapport_modtager']
    req_note = df_request.loc[0, 'Note_forespørgsel']
    req_id = df_request.loc[0, 'Id']
    req_dato = df_request.loc[0, 'Dato']
    req_ordrelationstype = df_request.loc[0, 'Ordrerelationstype']
    script_name = 'Sporbarhed_opspræt.py'
    timestamp = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    df_sections = ssf.get_ds_reporttype(req_id)
    # Formated date strings relevant for silos
    silo_req_date_dmy = req_dato.strftime('%d-%m-%Y')
    silo_req_date_ymd = ssf.convert_date_format(silo_req_date_dmy, 'dd-mm-yyyy', 'yyyy-mm-dd')
    silo_last_empty_ymd = ssf.rework.get_silo_last_empty(req_reference_no, silo_req_date_ymd)
    silo_next_empty_ymd = ssf.rework.get_silo_next_empty(req_reference_no, silo_req_date_ymd)
    # Read setup for section for reporttypes. NAV querys with NOLOCK to prevent deadlocks
    df_sections = ssf.get_ds_reporttype(req_type)
 
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
    # Update request that it is initiated and write into log
    # =============================================================================
    cursor_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                      SET [Forespørgsel_igangsat] = getdate()
                      WHERE [Id] = {req_id} """)
    cursor_ds.commit()
    ssf.log_insert(script_name, f'Request id: {req_id} initiated')
                           
                           
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
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_generelt))                   

    # =============================================================================
    # Section 9: Rework anvendt
    # =============================================================================
    section_id = 9
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = []    
    
    
    
    
    # =============================================================================
    # Section 23: Rework indgår i     
    # =============================================================================
    section_id = 23
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = []
    df_rework_used_in = ssf.rework.get_rework_orders_from_dates(req_reference_no, silo_last_empty_ymd, silo_next_empty_ymd)
    print(df_rework_used_in)
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

    # Save file
    excel_writer.save()
    ssf.log_insert(script_name, f'Excel file {file_name} created')

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
    ssf.log_insert(script_name, f'Request id: {req_id} inserted into [trc].[Email_log]')

    # =============================================================================
    # Update request that dataprocessing has been completed
    # =============================================================================
    cursor_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                      SET Data_færdigbehandlet = 1
                      WHERE [Id] = {req_id}""")
    cursor_ds.commit()
    ssf.log_insert(script_name, f'Request id: {req_id} completed')

    # Exit script
    raise SystemExit(0)