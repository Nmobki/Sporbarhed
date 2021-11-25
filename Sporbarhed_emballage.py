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
        df_orders = pd.read_sql(query_ds_lot_ventil, con_ds)
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
            # Lookups for field values
            df_orders['Referencenummer'] = req_reference_no + '\n (Nr.' + df_orders['Rullenummer'] + ') ' if req_type == 4 else req_reference_no
            df_orders['Navn'] = df_orders['Varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_orders.rename(columns={'Produktionsordrenummer':'Relateret ordre'}, inplace=True)
            df_orders['Relateret vare'] = df_orders['Relateret ordre'].apply(lambda x: ssf.get_nav_order_info(x))
            df_orders['Relateret navn'] = df_orders['Relateret vare'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_orders['Kilde'] = 'BKI_Datastore' if req_type == 6 else 'Navision'
            # Prepare dataframe for insert into Excel
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
    # Section 8: Massebalance
    # =============================================================================
    section_id = 8
    section_name = ssf.get_section_name(section_id, df_sections)
    columns_1_dec = ['[1] Færdigvaretilgang','[2] Salg','[3] Regulering & ompak',
                     '[4] Restlager','[5] Difference']
    columns_2_pct = ['[6] Difference pct']

    dict_massebalance = {'[1] Færdigvaretilgang': df_nav_færdigvaretilgang['Produceret'].sum(),
                         '[2] Salg': df_nav_færdigvaretilgang['Salg'].sum(),
                         '[3] Regulering & ompak': df_nav_færdigvaretilgang['Regulering & ompak'].sum(),
                         '[4] Restlager': df_nav_færdigvaretilgang['Restlager'].sum(),
                         '[5] Difference': None,
                         '[6] Difference pct': None
                         }
    dict_massebalance['[5] Difference'] = ( dict_massebalance['[1] Færdigvaretilgang'] - dict_massebalance['[2] Salg']
                                         - dict_massebalance['[3] Regulering & ompak'] - dict_massebalance['[4] Restlager'] )
    dict_massebalance['[6] Difference pct'] = ssf.zero_division(dict_massebalance['[5] Difference'], dict_massebalance['[1] Færdigvaretilgang'], 'None')

      #Number formating
    for col in columns_1_dec:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'dec_1')
    for col in columns_2_pct:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'pct_2')

    df_massebalance = pd.DataFrame.from_dict(data=dict_massebalance, orient='index').reset_index()
    df_massebalance.columns = ['Sektion','Værdi']
    df_massebalance['Note'] = [None, None, None, None, '[1] - [2] - [3] - [4]', '[5] / [1]']
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
    # Section 12: Karakterer
    # =============================================================================
    section_id = 12
    section_name = ssf.get_section_name(section_id, df_sections)
    columns_1_dec = ['Syre','Krop','Aroma','Eftersmag','Robusta']

    query_ds_karakterer = f""" SELECT [Referencenummer] AS [Produktionsordrenummer],
                               [Id] ,[Dato] ,[Bruger] ,[Smag_Syre] AS [Syre]
                              ,[Smag_Krop] AS [Krop] ,[Smag_Aroma] AS [Aroma] 
                              ,[Smag_Eftersmag] AS [Eftersmag],[Smag_Robusta] AS [Robusta] ,[Bemærkning]
                              FROM [cof].[Smageskema]
                              WHERE [Referencetype] = 2	
                                  AND [Referencenummer] IN ({req_orders_total})
                                  AND COALESCE([Smag_Syre],[Smag_Krop],[Smag_Aroma],
                                    [Smag_Eftersmag],[Smag_Robusta]) IS NOT NULL"""
    df_karakterer = pd.read_sql(query_ds_karakterer, con_ds)

    if ssf.get_section_status_code(df_karakterer) == 99:
        try:
            # Column formating
            df_karakterer['Dato'] = df_karakterer['Dato'].dt.strftime('%d-%m-%Y')
            for col in columns_1_dec:
                df_karakterer[col] = df_karakterer[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_karakterer, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_karakterer))

    # =============================================================================
    # Section 15: Lotnumre
    # =============================================================================
    section_id = 15
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Produktionsordrenummer', 'Lotnummer', 'Pallenummer', 'Produktionstidspunkt', 
                    'Kontrolleret af', 'Kontrol bemærkning', 'Kontroltidspunkt', 'Kilo', 
                    'Antal poser', 'Antal leakers', 'Leakers pct', 'Resultat af kontrol']
    columns_0_dec = ['Antal poser','Antal leakers']
    columns_1_dec = ['Kilo']
    columns_2_pct = ['Leakers pct']

    # Get all lotnumbers from orders from Navision
    query_nav_lotnos = f""" SELECT ILE.[Lot No_] AS [Lotnummer], ILE.[Order No_] AS [Produktionsordrenummer]
                	  ,LI.[Certificate Number] AS [Pallenummer]
                  	  ,[Quantity] * I.[Net Weight] AS [Kilo]
                	  ,CAST(ROUND(ILE.[Quantity] / IUM.[Qty_ per Unit of Measure],0) AS INT) AS [Antal poser]
                	  ,DATEADD(hour, 1, ILE.[Produktionsdato_-tid]) AS [Produktionstidspunkt]
                      FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) ILE
                      INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
                          ON ILE.[Item No_] = I.[No_]
                      LEFT JOIN [dbo].[BKI foods a_s$Lot No_ Information] (NOLOCK) AS LI
                    	  ON ILE.[Lot No_] = LI.[Lot No_]
                          AND ILE.[Item No_] = LI.[Item No_]
                      LEFT JOIN [dbo].[BKI foods a_s$Item Unit of Measure] (NOLOCK) AS IUM
                    	  ON ILE.[Item No_] = IUM.[Item No_]
                          AND IUM.[Code] = 'PS'
                      WHERE ILE.[Order Type] = 1
                    	  AND ILE.[Entry Type] = 6
                          AND ILE.[Order No_] IN ({req_orders_total}) """
    df_nav_lotnos = pd.read_sql(query_nav_lotnos, con_nav)
    # Get vac checks from BKI Datastore
    query_ds_vacslip = """ SELECT [Registreringstidspunkt] AS [Kontroltidspunkt]
                   ,[Initialer] AS [Kontrolleret af],[Lotnummer]
                   ,[Pallenummer],[Antal_poser] AS [Antal leakers]
                   ,[Bemærkning] AS [Kontrol bemærkning]
				   ,CASE WHEN [Overført_email_log] = 1 THEN
				   'Over grænseværdi' ELSE 'Ok' END AS [Resultat af kontrol]
                   FROM [cof].[Vac_slip] """
    df_ds_vacslip = pd.read_sql(query_ds_vacslip, con_ds)


    if ssf.get_section_status_code(df_nav_lotnos) == 99:
        try:
            df_nav_lotnos = pd.merge(df_nav_lotnos, df_ds_vacslip, left_on = 'Lotnummer',
                                    right_on = 'Lotnummer', how='left', suffixes=('', '_y'))
            df_nav_lotnos['Antal leakers'].fillna(value=0, inplace=True)
            df_nav_lotnos['Resultat af kontrol'].fillna(value='Ej kontrolleret', inplace=True)
            df_nav_lotnos['Leakers pct'] = df_nav_lotnos.apply(lambda x: ssf.zero_division(x['Antal leakers'], x['Antal poser'], 'Zero'), axis=1)
            df_nav_lotnos['Pallenummer'] = df_nav_lotnos['Pallenummer_y'].fillna(df_nav_lotnos['Pallenummer'])
            df_nav_lotnos['Produktionstidspunkt'] = df_nav_lotnos['Produktionstidspunkt'].dt.strftime('%d-%m-%Y %H:%M')
            df_nav_lotnos = df_nav_lotnos[column_order]
            # Data formating
            for col in columns_1_dec:
                df_nav_lotnos[col] = df_nav_lotnos[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            # Data formating
            for col in columns_0_dec:
                df_nav_lotnos[col] = df_nav_lotnos[col].apply(lambda x: ssf.number_format(x, 'dec_0'))
            # Data formating
            for col in columns_2_pct:
                df_nav_lotnos[col] = df_nav_lotnos[col].apply(lambda x: ssf.number_format(x, 'pct_2'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_nav_lotnos, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_nav_lotnos))

    # =============================================================================
    # Section 16: Reference- og henstandsprøver
    # =============================================================================
    section_id = 16
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Id', 'Registreringstidspunkt', 'Operatør', 'Silo', 'Prøvetype',
                    'Bemærkning', 'Smagning status', 'Antal prøver']
    
    # Query to get all samples registrered for the requested order.
    # Dataframe to be filtered later on to split by sample type.
    query_ds_samples = f""" SELECT KP.[Id],KP.[Ordrenummer],KP.[Registreringstidspunkt]
                	   ,KP.[Registreret_af] AS [Operatør],KP.[Bemærkning]
                       ,KP.[Prøvetype] AS [Prøvetype int],P.[Beskrivelse] AS [Prøvetype]
                       ,CASE WHEN KP.[Kontrol_mærkning] = 1 THEN 'Ok' 
                       WHEN KP.[Kontrol_mærkning] = 0 THEN 'Ej ok' END AS [Mærkning]
                       ,CASE WHEN KP.[Kontrol_rygsvejning] = 1	THEN 'Ok'
                       WHEN KP.[Kontrol_rygsvejning] = 0 THEN 'Ej ok' END AS [Rygsvejsning]
                       ,CASE WHEN KP.[Kontrol_ventil] = 1 THEN 'Ok'
                       WHEN KP.[Kontrol_ventil] = 0 THEN 'Ej ok' END AS [Ventil]
                       ,CASE WHEN KP.[Kontrol_peelbar] = 1	THEN 'Ok'
                       WHEN KP.[Kontrol_peelbar] = 0 THEN 'Ej ok' END AS [Peelbar]
                       ,CASE WHEN KP.[Kontrol_tintie] = 1 THEN 'Ok'
                       WHEN KP.[Kontrol_tintie] = 0 THEN 'Ej ok' END AS [Tintie]
    				   ,CASE WHEN KP.[Kontrol_tæthed] = 1 THEN 'Ok'
                       WHEN KP.[Kontrol_tæthed] = 0 THEN 'Ej ok' END AS [Tæthed]
                       ,KP.[Vægt_aflæst] AS [Vægt],KP.[Kontrol_ilt] / 100.0 AS [Ilt],KP.[Silo]
                       ,CASE WHEN SK.[Status] = 1 THEN 'Godkendt' WHEN SK.[Status] = 0
                       THEN 'Afvist' ELSE 'Ej smagt' END AS [Smagning status]
    				   ,KP.[Antal_prøver] AS [Antal prøver]
                       FROM [cof].[Kontrolskema_prøver] AS KP
                       INNER JOIN [cof].[Prøvetype] AS P 
                           ON KP.[Prøvetype] = P.[Id]
                       LEFT JOIN [cof].[Smageskema] AS SK
                           ON KP.[Id] = SK.[Id_org]
                           AND SK.[Id_org_kildenummer] = 6
                       WHERE KP.[Ordrenummer] IN ({req_orders_total}) """
    df_prøver = pd.read_sql(query_ds_samples, con_ds)
    
    df_temp = df_prøver[df_prøver['Prøvetype int'] != 0]

    if ssf.get_section_status_code(df_temp) == 99:
        try:
            df_temp['Registreringstidspunkt'] = df_temp['Registreringstidspunkt'].dt.strftime('%d-%m-%Y %H:%M')
            df_temp = df_temp[column_order]
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_temp))

    # =============================================================================
    # Section 17: Udtagne kontrolprøver
    # =============================================================================
    section_id = 17
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Id','Registreringstidspunkt', 'Operatør', 'Bemærkning',
                    'Mærkning', 'Rygsvejsning', 'Tæthed', 'Ventil', 'Peelbar',
                    'Tintie', 'Vægt', 'Ilt']
    columns_2_dec = ['Vægt']
    columns_0_pct = ['Ilt']
    df_temp = df_prøver[df_prøver['Prøvetype int'] == 0]

    if ssf.get_section_status_code(df_temp) == 99:
        try:
            df_temp['Registreringstidspunkt'] = df_temp['Registreringstidspunkt'].dt.strftime('%d-%m-%Y %H:%M')
            df_temp = df_temp[column_order]
            # Data formating
            for col in columns_2_dec:
                df_temp[col] = df_temp[col].apply(lambda x: ssf.number_format(x, 'dec_2'))
            # Data formating
            for col in columns_0_pct:
                df_temp[col] = df_temp[col].apply(lambda x: ssf.number_format(x, 'pct_0'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_temp))

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
