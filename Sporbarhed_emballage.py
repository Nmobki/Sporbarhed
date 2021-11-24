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
                        GROUP BY POAC.[Prod_ Order No_],POC.[Item No_], [Roll No_] """
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
                            GROUP BY POAC.[Prod_ Order No_],POC.[Item No_], [Roll No_] """
    # Query Navision for information with refencetype = 5 (purchase order) and no roll number. 
    query_nav_purch = f"""SELECT POAC.[Prod_ Order No_] AS [Produktionsordrenummer]
                    	,POC.[Item No_] AS [Varenummer], [Roll No_] AS [Rullenummer]
                        FROM [dbo].[BKI foods a_s$Prod_ Order Add_ Comp_] AS POAC
                        INNER JOIN [dbo].[BKI foods a_s$Prod_ Order Component] AS POC
                        	ON POAC.[Prod_ Order No_] = POC.[Prod_ Order No_]
                        	AND POAC.[Prod_ Order Line No_] = POC.[Prod_ Order Line No_]
                        	AND POAC.[Prod_ Order Component Line No_] = POC.[Line No_]
                        WHERE POAC.[Batch_Lot No_] = '{req_reference_no}'
                        GROUP BY POAC.[Prod_ Order No_],POC.[Item No_], [Roll No_] """
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
                            GROUP BY POAC.[Prod_ Order No_],POC.[Item No_],[Roll No_] """
    # Query BKI_Datastore for information with referencetype = 4 (lot no), only for ventil components
    query_ds_lot_ventil = f"""SELECT [Ordrenummer] AS [Produktionsordrenummer], [Varenummer]
                              ,NULL AS [Rullenummer]
                              FROM [cof].[Ventil_registrering]
                              WHERE [Batchnr_stregkode] = '{req_reference_no}'
                              GROUP BY [Varenummer], [Ordrenummer]"""
    # Read any of the above queries into a dataframe, depending on reference type, request type and whether any roll number has been entered.
    if req_type == 4: # Folie
        if req_reference_type == 4: # Lotnumber
            if req_roll: # req_roll holds a value
                df_orders = pd.read_sql(query_nav_lot_roll, con_nav)
            else: # req_roll doesn't hold a value
                df_orders = pd.read_sql(query_nav_lot, con_nav)
        if req_reference_type == 5: # Purchase order
            if req_roll: # req_roll holds a value
                df_orders = pd.read_sql(query_nav_purch_roll, con_nav)
            else: # req_roll doesn't hold a value
                df_orders = pd.read_sql(query_nav_purch, con_nav)
    elif req_type == 5: # Karton
        df_orders = pd.read_sql(query_nav_lot, con_nav)
    elif req_type == 6: # Ventil
        df_orders = pd.read_sql(query_ds_lot_ventil, con_nav)
    else:
        df_orders = pd.DataFrame()


    req_orders_total = ssf.string_to_sql(df_orders['Produktionsordrenummer'].unique().tolist())
    # Get a string with all lotnumbers produced directly or indirectly using any of the identified orders
    nav_lotnots_total_sql_string = ssf.finished_goods.get_nav_lotnos_from_orders(req_orders_total, 'string')
    # Get information about each production order based on lotnumbers identified above
    df_nav_færdigvaretilgang = ssf.finished_goods.get_production_information(nav_lotnots_total_sql_string)
    # Get information about any sales to any customers based on lotnumbers identified above
    df_nav_debitorer = ssf.finished_goods.get_sales_information(nav_lotnots_total_sql_string)


    # =============================================================================
    # Section 1: Generelt
    # =============================================================================
    section_id = 1
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Basisenhed','Lotnummer','Købsordre',
                    'Rullenummer','Leverandørnummer','Leverandørnavn']

    df_generelt = df_request
    if ssf.get_section_status_code(df_generelt) == 99:
        try:
            # Values fetched from initial request
            df_generelt['Lotnummer'] = req_reference_no if req_reference_type == 4 else ''
            df_generelt['Købsordre'] = req_reference_no if req_reference_type == 5 else ''
            df_generelt['Rullenummer'] = req_roll
            # Lookup values through functions
            df_generelt['Varenummer'] = df_orders['Varenummer'].iloc[0] if len(df_orders) != 0 else ''
            df_generelt['Varenavn'] = df_generelt.apply(lambda x: ssf.get_nav_item_info(x.Varenummer, 'Beskrivelse'), axis = 1)
            df_generelt['Basisenhed'] = df_generelt.apply(lambda x: ssf.get_nav_item_info(x.Varenummer, 'Basisenhed'), axis = 1)
            df_generelt['Leverandørnummer'] = df_generelt.apply(lambda x: ssf.get_nav_item_info(x.Varenummer, 'Leverandørnummer'), axis = 1)
            df_generelt['Leverandørnavn'] = df_generelt.apply(lambda x: ssf.get_nav_vendor_info(x.Leverandørnummer, 'Navn'), axis = 1)
            # df_nav_lotno.apply(lambda x: ssf.zero_division(x['Antal leakers'], x['Antal poser'], 'Zero'), axis=1)
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

    # =============================================================================
    # Section 2: Relaterede ordrer NAV --> Probat
    # =============================================================================
    section_id = 2
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Referencenummer','Varenummer','Navn','Relateret ordre',
                    'Relateret vare','Relateret navn','Kilde']

    if ssf.get_section_status_code(df_orders) == 99:
        try:
            df_orders['Referencenummer'] = req_reference_no + '\n (Nr.' + df_orders['Rullenummer'] + ') ' if req_type == 4 else req_reference_no
            df_orders['Navn'] = df_orders['Varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_orders.rename(columns={'Produktionsordrenummer':'Relateret ordre'}, inplace=True)
            print(df_orders['Relateret ordre'])
            df_orders['Relateret vare'] = df_orders['Relateret ordre'].apply(lambda x: ssf.get_nav_order_info(x))
            df_orders['Relateret navn'] = df_orders['Relateret vare'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_orders['Kilde'] = 'BKI_Datastore' if req_type == 6 else 'Navision'
            df_orders = df_orders[column_order]
            df_orders.sort_values(by=['Referencenummer','Relateret ordre'], inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_orders, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
    # =================================================================
    # Section 19: Relation visualization
    # =================================================================
            #Try to create .png with relations illustrated and add to .docx as well
            try:
                df_order_relations = pd.DataFrame()
                df_order_relations['Primær'] = df_orders['Referencenummer']
                df_order_relations['Sekundær'] = df_orders['Relateret ordre']
                df_order_relations = df_order_relations[['Primær','Sekundær']]
                # Create relation visualization
                array_for_drawing = list(df_order_relations.itertuples(index=False, name=None))
                graph = nx.DiGraph()
                graph.add_edges_from(array_for_drawing)
                relations_plot = nx.drawing.nx_pydot.to_pydot(graph)
                relations_plot.write_png(path_png_relations)
                # Write to log
                ssf.section_log_insert(req_id, 19, 0)
            except Exception as e: # Insert error into log. Same section_id as others..
                ssf.section_log_insert(req_id, 19, 2, e)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_orders))

    # =============================================================================
    # Section 3: Færdigvaretilgang
    # =============================================================================
    section_id = 3
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Ordrenummer','Produceret','Salg','Restlager','Regulering & ompak']
    columns_1_dec = ['Produceret','Salg','Restlager','Regulering & ompak']
    columns_strip = ['Ordrenummer']

    if ssf.get_section_status_code(df_nav_færdigvaretilgang) == 99:
        try:
            # Concat order numbers to one string
            df_nav_færdigvaretilgang = df_nav_færdigvaretilgang.groupby(['Varenummer','Varenavn']).agg(
               {'Ordrenummer': lambda x: ','.join(sorted(pd.Series.unique(x))),
                'Produceret': 'sum',
                'Salg': 'sum',
                'Restlager': 'sum',
                'Regulering & ompak': 'sum'
               }).reset_index()
            # Remove trailing and leading commas
            for col in columns_strip:
                df_nav_færdigvaretilgang[col] = df_nav_færdigvaretilgang[col].apply(lambda x: ssf.strip_comma_from_string(x))
            # Create total for dataframe
            dict_færdigvare_total = {'Produceret': [df_nav_færdigvaretilgang['Produceret'].sum()],
                                     'Salg': [df_nav_færdigvaretilgang['Salg'].sum()],
                                     'Restlager': [df_nav_færdigvaretilgang['Restlager'].sum()],
                                     'Regulering & ompak': [df_nav_færdigvaretilgang['Regulering & ompak'].sum()]}
            df_temp_total = pd.concat([df_nav_færdigvaretilgang,
                                      pd.DataFrame.from_dict(data=dict_færdigvare_total, orient='columns')])
            df_temp_total = df_temp_total[column_order]
            df_temp_total.sort_values(by=['Varenummer'], inplace=True)
            # Data formating
            for col in columns_1_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_total, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_temp_total))

    # =============================================================================
    # Section 7: Debitorer
    # =============================================================================
    section_id = 7
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Debitornummer','Debitornavn','Dato','Varenummer','Varenavn','Produktionsordrenummer',
                        'Enheder','Kilo']
    columns_1_dec = ['Enheder','Kilo']
    columns_strip = ['Produktionsordrenummer']
    if ssf.get_section_status_code(df_nav_debitorer) == 99:
        try:
            # Concat Order nos to one string
            df_nav_debitorer = df_nav_debitorer.groupby(['Debitornummer','Debitornavn','Dato','Varenummer']).agg(
                {'Produktionsordrenummer': lambda x: ','.join(sorted(pd.Series.unique(x))),
                 'Enheder': 'sum',
                 'Kilo': 'sum'
                }).reset_index()
            # Remove trailing and leading commas
            for col in columns_strip:
                df_nav_debitorer[col] = df_nav_debitorer[col].apply(lambda x: ssf.strip_comma_from_string(x))
            # Create total for dataframe
            dict_debitor_total = {'Enheder': [df_nav_debitorer['Enheder'].sum()],
                                  'Kilo':[df_nav_debitorer['Kilo'].sum()]}
            # Add varenavn
            df_nav_debitorer['Varenavn'] = df_nav_debitorer['Varenummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
             # Look up column values and string format datecolumn for export
            df_nav_debitorer['Dato'] = df_nav_debitorer['Dato'].dt.strftime('%d-%m-%Y')
            # Create temp dataframe with total
            df_temp_total = pd.concat([df_nav_debitorer, pd.DataFrame.from_dict(data=dict_debitor_total, orient='columns')])
            df_temp_total = df_temp_total[column_order]
            df_temp_total.sort_values(by=['Varenummer','Debitornummer','Dato'], inplace=True)
            # Data formating
            for col in columns_1_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_total, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_nav_debitorer))

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
    cursor_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                      SET Data_færdigbehandlet = 1
                      WHERE [Id] = {req_id}""")
    cursor_ds.commit()
    # ssf.log_insert(script_name, f'Request id: {req_id} completed')

    # Exit script
    ssf.get_exit_check(0)
