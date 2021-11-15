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
    ssf.get_exit_check(len(df_request))

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
        except Exception as e:
            # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else:
        # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_generelt))

    # =============================================================================
    # Section 9: Rework anvendt
    # =============================================================================
    section_id = 9
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Produktionsordre','Indhold','Indhold varenummer',
                    'Indhold varenavn','Kilde']

    # Dataframe containing any grinding orders that have used rework - also used in section 23..
    df_rework_used_in = ssf.rework.get_rework_orders_from_dates(req_reference_no, silo_last_empty_ymd, silo_next_empty_ymd)
    # Create dataframe to query for all contents of rework silos using function
    df_rework_used = df_rework_used_in
    df_rework_used['Startdato'] = silo_last_empty_ymd
    df_rework_used['Slutdato'] = df_rework_used['Dato']
    df_rework_used['Silo'] = req_reference_no
    df_rework_used['Produktionsordre'] = df_rework_used['Ordrenummer']
    # Alter dataframe to contain results from function
    df_rework_used = ssf.rework.get_rework_total(df_rework_used)

    if ssf.get_section_status_code(df_rework_used) == 99:
        try:
            df_rework_used['Indhold varenummer'] = df_rework_used['Produktionsordre'].apply(lambda x: ssf.get_nav_order_info(x))
            df_rework_used['Indhold varenavn'] = df_rework_used['Indhold varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_rework_used = df_rework_used[column_order]
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_rework_used, section_name, True)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        # Insert error into log
        except Exception as e:
            ssf.section_log_insert(req_id, section_id, 2, e)
    else:
        # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_rework_used))

    # =============================================================================
    # Section 23: Rework indgår i
    # =============================================================================
    section_id = 23
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Dato','Ordrenummer','Varenummer','Varenavn','Kilo']
    columns_1_dec = ['Kilo']
    columns_strip = ['Dato']

    if ssf.get_section_status_code(df_rework_used_in) == 99:
        try:
            # String of order numbers used for SQL query
            rework_orders = ssf.string_to_sql(df_rework_used_in['Ordrenummer'].unique().tolist())
            # Format dates to string
            df_rework_used_in['Dato'] = df_rework_used_in['Dato'].dt.strftime('%d-%m-%Y')
            # Concat dates into one string
            df_rework_used_in = df_rework_used_in.groupby(['Ordrenummer']).agg(
                {'Dato': lambda x: ','.join(sorted(pd.Series.unique(x)))
                }).reset_index()
            # Get output from grinders
            query_probat_grinding_output = f""" SELECT [ORDER_NAME] AS [Ordrenummer]
                                   ,SUM([WEIGHT] / 1000.0) AS [Kilo]
                                   FROM [dbo].[PRO_EXP_ORDER_UNLOAD_G]
                                   WHERE [ORDER_NAME] IN ({rework_orders})
                                   GROUP BY [ORDER_NAME] """
            df_probat_grinding_output = pd.read_sql(query_probat_grinding_output, con_probat)
            # Join to input
            df_rework_used_in = pd.merge(df_rework_used_in,
                                         df_probat_grinding_output,
                                         left_on = 'Ordrenummer',
                                         right_on = 'Ordrenummer',
                                         how = 'left',
                                         suffixes = ('', '_R'))
            # Column formating and lookups
            for col in columns_strip:
                df_rework_used_in[col] = df_rework_used_in[col].apply(lambda x: ssf.strip_comma_from_string(x))
            df_rework_used_in['Varenummer'] = df_rework_used_in['Ordrenummer'].apply(lambda x: ssf.get_nav_order_info(x))
            df_rework_used_in['Varenavn'] = df_rework_used_in['Varenummer'].apply(ssf.get_nav_item_info, field = 'Beskrivelse')
            # Create total for dataframe
            dict_rework_used_in_total = {'Kilo': df_rework_used_in['Kilo'].sum()}
            # Add total to dataframe and format decimals on kilo column
            df_rework_used_in = pd.concat([df_rework_used_in, pd.DataFrame([dict_rework_used_in_total])])
            for col in columns_1_dec:
                df_rework_used_in[col] = df_rework_used_in[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            df_rework_used_in = df_rework_used_in[column_order]
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_rework_used_in, section_name, True)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        # Insert error into log
        except Exception as e:
            ssf.section_log_insert(req_id, section_id, 2, e)
    else:
        # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_rework_used_in))

    # =============================================================================
    # Section 3: Færdigvaretilgang
    # =============================================================================
    section_id = 3
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Ordrenummer','Produceret','Salg','Restlager','Regulering & ompak']
    columns_1_dec = ['Produceret','Salg','Restlager','Regulering & ompak']
    columns_strip = ['Ordrenummer']

    # Get order numbers for finished goods from Probat and Navision
    query_probat_order_relations = f""" SELECT [ORDER_NAME] AS [Ordrenummer]
                                        ,[S_ORDER_NAME] AS [Relateret ordre]
                                        ,'Probat PG' AS [Kilde]
                                        FROM [dbo].[PRO_EXP_ORDER_SEND_PG]
                                        WHERE [ORDER_NAME] <> ''
                                        	AND [S_ORDER_NAME] IN ({rework_orders})
                                        GROUP BY [ORDER_NAME],[S_ORDER_NAME] """
    df_probat_order_relations = pd.read_sql(query_probat_order_relations, con_probat)
    df_nav_order_relations = ssf.get_nav_orders_from_related_orders(rework_orders)
    # Concat lists and convert list of orders to string used for sql
    order_numbers_fg_sql = ssf.extend_order_list(req_ordrelationstype, [],
                                               df_probat_order_relations['Ordrenummer'].unique().tolist(),
                                               df_nav_order_relations['Ordrenummer'].unique().tolist())
    order_numbers_fg_sql = ssf.string_to_sql(order_numbers_fg_sql)
    # Get string of lotnots
    nav_lotnots = ssf.finished_goods.get_nav_lotnos_from_orders(order_numbers_fg_sql, 'string')
    # Get results from Navision
    df_nav_færdigvaretilgang = ssf.finished_goods.get_production_information(nav_lotnots)

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
            dict_færdigvare_total = {'Produceret': df_nav_færdigvaretilgang['Produceret'].sum(),
                                     'Salg': df_nav_færdigvaretilgang['Salg'].sum(),
                                     'Restlager': df_nav_færdigvaretilgang['Restlager'].sum(),
                                     'Regulering & ompak': df_nav_færdigvaretilgang['Regulering & ompak'].sum()
                                     }
            df_nav_færdigvaretilgang = pd.concat([df_nav_færdigvaretilgang,
                                                  pd.DataFrame(dict_færdigvare_total, index=[0])])
            df_nav_færdigvaretilgang = df_nav_færdigvaretilgang[column_order]
            df_nav_færdigvaretilgang.sort_values(by=['Varenummer'], inplace=True)
            # Data formating
            for col in columns_1_dec:
                df_nav_færdigvaretilgang[col] = df_nav_færdigvaretilgang[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_nav_færdigvaretilgang, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_nav_færdigvaretilgang))

    # =============================================================================
    # Section 7: Debitorer
    # =============================================================================
    section_id = 7
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Debitornummer','Debitornavn','Dato','Varenummer','Varenavn','Produktionsordrenummer',
                        'Enheder','Kilo']
    columns_1_dec = ['Enheder','Kilo']
    columns_strip = ['Produktionsordrenummer']

    df_nav_debitorer = ssf.finished_goods.get_sales_information(nav_lotnots)

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
    # Section 8: Massebalance
    # =============================================================================
    section_id = 8
    section_name = ssf.get_section_name(section_id, df_sections)
    columns_1_dec = ['[1] Kilo formalet','[2] Kilo færdigvarer','[3] Difference','[5] Salg',
                     '[6] Restlager','[7] Regulering & ompak','[8] Difference']
    columns_2_pct = ['[4] Difference pct','[9] Difference pct']

    dict_massebalance = {'[1] Kilo formalet': dict_rework_used_in_total['Kilo'],
                         '[2] Kilo færdigvarer': dict_færdigvare_total['Produceret'],
                         '[3] Difference': None,
                         '[4] Difference pct': None,
                         '[5] Salg': dict_færdigvare_total['Salg'],
                         '[6] Restlager': dict_færdigvare_total['Restlager'],
                         '[7] Regulering & ompak': dict_færdigvare_total['Regulering & ompak'],
                         '[8] Difference': None,
                         '[9] Difference pct': None
                        }
    # Calculate differences and percentages before converting to dataframe:
    dict_massebalance['[3] Difference'] = dict_massebalance['[1] Kilo formalet'] - dict_massebalance['[2] Kilo færdigvarer']
    dict_massebalance['[4] Difference pct'] = ssf.zero_division(dict_massebalance['[3] Difference'], dict_massebalance['[1] Kilo formalet'], 'None')
    dict_massebalance['[8] Difference'] = ( dict_massebalance['[2] Kilo færdigvarer'] - dict_massebalance['[5] Salg']
                                            - dict_massebalance['[6] Restlager'] - dict_massebalance['[7] Regulering & ompak'] )
    dict_massebalance['[9] Difference pct'] = ssf.zero_division(dict_massebalance['[8] Difference'], dict_massebalance['[2] Kilo færdigvarer'], 'None')
    # Number formating
    for col in columns_1_dec:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'dec_1')
    for col in columns_2_pct:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'pct_2')

    df_massebalance = pd.DataFrame.from_dict(data=dict_massebalance, orient='index').reset_index()
    df_massebalance.columns = ['Sektion','Værdi']
    df_massebalance['Note'] = [None,None,'[1]-[2]','[3]/[1]',None,None,None,'[2]-[5]-[6]-[7]','[8]/[2]']
    df_massebalance['Bemærkning'] = None

    if ssf.get_section_status_code(df_massebalance) == 99:
        try:
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_massebalance, section_name, True)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_massebalance))


    # =============================================================================
    # Section 2: Relaterede ordrer
    # =============================================================================
    section_id = 2
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Ordrenummer','Varenummer','Navn','Relateret ordre',
                    'Relateret vare','Relateret navn','Kilde']

    if req_ordrelationstype == 0: # All relations
        df_temp_orders = pd.concat([df_nav_order_relations,df_probat_order_relations])
    elif req_ordrelationstype == 1: # Only Probat
        df_temp_orders = df_probat_order_relations
    elif req_ordrelationstype == 2: # Only Navision
        df_temp_orders = df_nav_order_relations

    if ssf.get_section_status_code(df_temp_orders) == 99:
        try:
            df_temp_orders['Varenummer'] = df_temp_orders['Ordrenummer'].apply(lambda x: ssf.get_nav_order_info(x))
            df_temp_orders['Navn'] = df_temp_orders['Varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_temp_orders['Relateret vare'] = df_temp_orders['Relateret ordre'].apply(lambda x: ssf.get_nav_order_info(x))
            df_temp_orders['Relateret navn'] = df_temp_orders['Relateret vare'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            # Remove orders not existing in NAV and sort columns and rows
            df_temp_orders = df_temp_orders[column_order]
            df_temp_orders.sort_values(by=['Ordrenummer','Relateret ordre'], inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_orders, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
    # =================================================================
    # Section 19: Relation visualization
    # =================================================================
            #Try to create .png with relations illustrated
            try:
                df_temp_order_relation = df_temp_orders[['Ordrenummer','Varenummer','Relateret ordre','Relateret vare']]
                df_temp_order_relation['Ordretype'] = df_temp_order_relation['Varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Varetype'))
                df_temp_order_relation['Relateret ordretype'] = df_temp_order_relation['Relateret vare'].apply(lambda x: ssf.get_nav_item_info(x, 'Varetype'))
                df_temp_order_relation['Primær'] = df_temp_order_relation['Ordretype'] + '\n' + df_temp_order_relation['Ordrenummer']
                df_temp_order_relation['Sekundær'] = df_temp_order_relation['Relateret ordretype'] + '\n' + df_temp_order_relation['Relateret ordre']
                df_temp_order_relation = df_temp_order_relation[['Primær','Sekundær']]
                # Prepare and add source of rework to main plot
                df_temp_rework_relation = df_rework_used
                df_temp_rework_relation['Primær'] = 'Rework kilde\n' + df_temp_rework_relation['Kilde']
                df_temp_rework_relation['Sekundær'] = df_temp_rework_relation['Produktionsordre']
                df_temp_rework_relation = df_temp_rework_relation[['Primær','Sekundær']]
                df_temp_order_relation = pd.concat([df_temp_order_relation,df_temp_rework_relation])
                # Create relation visualization
                array_for_drawing = list(df_temp_order_relation.itertuples(index=False, name=None))
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
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_temp_orders))


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
        # Insert error into log
        except Exception as e:
            ssf.section_log_insert(req_id, section_id, 2, e)
    else:
        # Write into log if no data is found or section is out of scope
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
    ssf.get_exit_check(0)
