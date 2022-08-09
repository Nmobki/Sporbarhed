#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import networkx as nx
import Sporbarhed_shared_functions as ssf
import Sporbarhed_shared_finished_goods as ssfg
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
    req_reference_no = df_request.loc[0, 'Referencenummer'].rstrip(' ')
    req_recipients = df_request.loc[0, 'Rapport_modtager']
    req_note = df_request.loc[0, 'Note_forespørgsel']
    req_id = df_request.loc[0, 'Id']
    req_modtagelse = df_request.loc[0, 'Modtagelse']
    req_ordrelationstype = df_request.loc[0, 'Ordrerelationstype']

    script_name = 'Sporbarhed_råkaffe.py'
    orders_top_level = [req_reference_no]
    orders_related = []
    df_sections = ssf.get_ds_reporttype(req_id)
    # Read setup for section for reporttype
    df_sections = ssf.get_ds_reporttype(req_type)
    # =============================================================================
    # Update request that it is initiated and write into log
    # =============================================================================
    sssi.con_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                      SET [Forespørgsel_igangsat] = getdate()
                      WHERE [Id] = {req_id} """)
    ssf.log_insert(script_name, f'Request id: {req_id} initiated')

    # =============================================================================
    # Variables for files generated
    # =============================================================================
    filepath = sssi.report_filepath
    file_name = f'Rapport_{req_reference_no}_{req_id}'
    # Excel workbook
    wb_name = f'{file_name}.xlsx'
    path_file_wb = filepath + r'\\' + wb_name
    excel_writer = pd.ExcelWriter(path_file_wb, engine='xlsxwriter')
    # Relationship diagram
    png_relations_name = f'{file_name}.png'
    path_png_relations = filepath + r'\\' + png_relations_name

    # =============================================================================
    # Data fetching for sections throughout script
    # =============================================================================

    # General info from Navision
    query__nav_generelt = f""" SELECT TOP 1 PL.[Buy-from Vendor No_] AS [Leverandørnummer]
                    	,V.[Name] AS [Leverandørnavn] ,PL.[No_] AS [Varenummer]
                        ,I.[Description] AS [Varenavn] ,I.[Mærkningsordning]
						,CR.[Name] +' (' +PH.[Pay-to Country_Region Code] + ')' AS [Oprindelsesland]
                        FROM [dbo].[BKI foods a_s$Purchase Line] AS PL
                        INNER JOIN [dbo].[BKI foods a_s$Item] AS I
                            ON PL.[No_] = I.[No_]
						INNER JOIN [dbo].[BKI foods a_s$Purchase Header] AS PH
							ON PL.[Document No_] = PH.[No_]
                        LEFT JOIN [dbo].[BKI foods a_s$Vendor] AS V
                            ON PL.[Buy-from Vendor No_] = V.[No_]
						LEFT JOIN [dbo].[BKI foods a_s$Country_Region] AS CR
							ON PH.[Pay-to Country_Region Code] = CR.[Code]
                        WHERE PL.[Type] = 2
                            AND PL.[Document No_] = '{req_reference_no}' """
    df_nav_generelt = pd.read_sql(query__nav_generelt, sssi.con_nav)

    # Get timestamp for last export of inventory from Probat
    query_probat_inventory_timestamp = """ WITH [Tables] AS (
                                       SELECT MAX([RECORDING_DATE]) AS [Date]
                                        FROM [dbo].[PRO_EXP_PRODUCT_POS_INVENTORY]
                                        UNION ALL
                                        SELECT MAX([RECORDING_DATE])
                                        FROM [dbo].[PRO_EXP_WAREHOUSE_INVENTORY] )
                                        SELECT MIN([Date]) AS [Silobeholdning eksporteret]
                                        FROM [Tables] """
    df_probat_inventory_timestamp = pd.read_sql(query_probat_inventory_timestamp, sssi.con_probat)

    # Information from Probat for the receiving of coffee
    query_probat_receiving = f""" IF '{req_modtagelse}' = 'None' -- Modtagelse ikke tastet
                             BEGIN
                             SELECT	CAST([DESTINATION] AS VARCHAR(20)) AS [Placering]
                             ,[RECORDING_DATE] AS [Dato] ,[PAPER_VALUE] / 10.0 AS [Kilo]
                    		 ,NULL AS [Restlager]
                             FROM [dbo].[PRO_EXP_REC_ARRIVE]
                        	 WHERE CAST([CONTRACT_NO] AS VARCHAR(20)) = '{req_reference_no}'
                        	 UNION ALL
                        	 SELECT [Placering] ,NULL ,NULL ,SUM([Kilo]) AS [Kilo]
                        	 FROM [dbo].[Newest total inventory]
                        	 WHERE [Kontrakt] = '{req_reference_no}' 
                             AND [Placering] NOT LIKE '2__'
                        	 GROUP BY [Placering]
                             END
                             IF '{req_modtagelse}' <> 'None' -- Modtagelse er udfyldt
                             BEGIN
                             SELECT CAST([DESTINATION] AS VARCHAR(20)) AS [Placering]
                             ,[RECORDING_DATE] AS [Dato] ,[PAPER_VALUE] / 10.0 AS [Kilo]
                             ,NULL AS [Restlager]
                             FROM [dbo].[PRO_EXP_REC_ARRIVE]
                             WHERE CAST([CONTRACT_NO] AS VARCHAR(20)) = '{req_reference_no}'
                             AND CAST([DELIVERY_NAME] AS VARCHAR(20)) = '{req_modtagelse}'
                             UNION ALL
                             SELECT [Placering] ,NULL ,NULL ,SUM([Kilo]) AS [Kilo]
                             FROM [dbo].[Newest total inventory]
                             WHERE [Kontrakt] = '{req_reference_no}' 
                             AND CAST([Modtagelse] AS VARCHAR(20)) = '{req_modtagelse}'
                             AND [Placering] NOT LIKE '2__'
                             GROUP BY [Placering] END """
    df_probat_receiving = pd.read_sql(query_probat_receiving, sssi.con_probat)

    # Information from Probat for the processing of coffee
    query_probat_processing = f""" IF '{req_modtagelse}' = 'None' -- Ingen modtagelse tastet
                              BEGIN
                              SELECT [DESTINATION] AS [Silo]
                              ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ), 0) AS [Dato]
                              ,SUM([WEIGHT] / 10.0) AS [Kilo]
                              ,0 AS [Restlager], [CUSTOMER_CODE] AS [Sortnummer]
                              FROM [dbo].[PRO_EXP_REC_SUM_DEST]
                              WHERE [CONTRACT_NO] = '{req_reference_no}' AND [DESTINATION] LIKE '2__'
                              GROUP BY [DESTINATION] ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ) ,0), [CUSTOMER_CODE]
                              UNION ALL
                              SELECT [Placering] ,NULL ,0 ,SUM([Kilo]),[Varenummer] AS [Sortnummer]
                              FROM [dbo].[Newest total inventory]
                              WHERE [Kontrakt] = '{req_reference_no}' AND [Placering]  LIKE '2__'
                              GROUP BY [Placering],[Varenummer]
                              END
                              IF '{req_modtagelse}' <> 'None' -- Modtagelse tastet
                              BEGIN
                              SELECT [DESTINATION] AS [Silo]
                              ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ), 0) AS [Dato]
                              ,SUM([WEIGHT] / 10.0) AS [Kilo]
                              ,0 AS [Restlager], [CUSTOMER_CODE] AS [Sortnummer]
                              FROM [dbo].[PRO_EXP_REC_SUM_DEST]
                              WHERE [CONTRACT_NO] = '{req_reference_no}'
                              AND [DESTINATION] LIKE '2__'
                              AND [DELIVERY_NAME] = '{req_modtagelse}'
                              GROUP BY [DESTINATION] ,DATEADD(D, DATEDIFF(D, 0, [START_TIME] ) ,0), [CUSTOMER_CODE]
                              UNION ALL
                              SELECT [Placering] ,NULL ,0
                              ,SUM([Kilo]) AS [Kilo],[Varenummer] AS [Sortnummer]
                              FROM [dbo].[Newest total inventory]
                              WHERE [Kontrakt] = '{req_reference_no}' AND [Placering]  LIKE '2__'
                              AND [Modtagelse] = '{req_modtagelse}'
                              GROUP BY [Placering],[Varenummer]
                              END """
    df_probat_processing = pd.read_sql(query_probat_processing, sssi.con_probat)

    # Get order numbers the requested coffee has been used in
    query_probat_used_in_roast = f""" IF '{req_modtagelse}' = 'None' -- Ingen modtagelse tastet
                               BEGIN
                               SELECT [ORDER_NAME]
                               FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                               WHERE [S_CONTRACT_NO] = '{req_reference_no}'
                               GROUP BY [ORDER_NAME]
                               END
                               IF '{req_modtagelse}' <> 'None' -- Modtagelse tastet
                               BEGIN
                               SELECT [ORDER_NAME]
                               FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                               WHERE [S_CONTRACT_NO] = '{req_reference_no}'
                               AND [S_DELIVERY_NAME] = '{req_modtagelse}'
                               GROUP BY [ORDER_NAME]
                               END """
    df_probat_used_in_roast = pd.read_sql(query_probat_used_in_roast, sssi.con_probat)

    # Convert orders to string for use in later queries
    roast_orders = df_probat_used_in_roast['ORDER_NAME'].unique().tolist()
    sql_roast_orders = ssf.string_to_sql(roast_orders)

    # Green coffee used for roasting
    query_probat_roast_input = f""" IF '{req_modtagelse}' = 'None'
                                BEGIN
                                SELECT [CUSTOMER_CODE] AS [Varenummer]
                                ,[ORDER_NAME] AS [Ordrenummer]
                                ,[DESTINATION] AS [Rister]
                                ,SUM(CASE WHEN [S_CONTRACT_NO] = '{req_reference_no}'
                                    THEN [WEIGHT] / 1000.0 ELSE 0 END) 
                                    AS [Heraf kontrakt]
                                ,SUM([WEIGHT] / 1000.0) AS [Kilo råkaffe]
                                FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                                WHERE [ORDER_NAME] IN ({sql_roast_orders})
                                GROUP BY  [CUSTOMER_CODE] 
                                ,[ORDER_NAME] ,[DESTINATION]
                                END
                                IF '{req_modtagelse}' <> 'None'
                                BEGIN
                                SELECT [CUSTOMER_CODE] AS [Varenummer]
                                ,[ORDER_NAME] AS [Ordrenummer]
                                ,[DESTINATION] AS [Rister]
                                ,SUM(CASE WHEN [S_CONTRACT_NO] = '{req_reference_no}' 
                                     AND [S_DELIVERY_NAME] = '{req_modtagelse}'
                              	   THEN [WEIGHT] / 1000.0
                              	   ELSE 0 END) AS [Heraf kontrakt]
                                ,SUM([WEIGHT] / 1000.0) AS [Kilo råkaffe]
                                FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                                WHERE [ORDER_NAME] IN ({sql_roast_orders})
                                GROUP BY  [CUSTOMER_CODE]
                                ,[ORDER_NAME] ,[DESTINATION] END """
    # Only try to read query if any orders exist
    if len(sql_roast_orders) > 0:
        df_probat_roast_input = pd.read_sql(query_probat_roast_input, sssi.con_probat)
    else:
        df_probat_roast_input = pd.DataFrame()

    # Output from roasters
    query_probat_roast_output = f""" WITH SAMPLES AS ( SELECT [ORDER_NAME]
                                ,AVG(CASE WHEN [COLOR_NEW]  = 0 THEN NULL ELSE [COLOR_NEW] END / 100.0) AS [Farve ny]
                                ,AVG(CASE WHEN [COLOR_OLD]  = 0 THEN NULL ELSE [COLOR_OLD] END / 100.0) AS [Farve gl]
                                ,AVG([END_TEMP] / 10.0) AS [Slut temp]
                                ,AVG([WATER] / 10.0) AS [L vand]
                                ,AVG(CASE WHEN [HUMIDITY] = 0 THEN NULL ELSE [HUMIDITY] END / 10000.0) AS [Vandpct] 
                            	,COUNT(*) AS [Antal samples]
                                FROM [dbo].[PRO_EXP_SAMPLE_ROASTER]
                                WHERE [PRO_EXPORT_GENERAL_ID] IN (SELECT MAX([PRO_EXPORT_GENERAL_ID]) FROM [dbo].[PRO_EXP_SAMPLE_ROASTER] GROUP BY [SAMPLE_ID])
                                GROUP BY [ORDER_NAME] )
                                SELECT
                                DATEADD(D, DATEDIFF(D, 0, ULR.[RECORDING_DATE] ), 0) AS [Dato]
                                ,ULR.[DEST_NAME] AS [Silo] ,ULR.[ORDER_NAME] AS [Ordrenummer]
                                ,SUM(ULR.[WEIGHT]) / 1000.0 AS [Kilo ristet]
								,S.[Antal samples], S.[Farve gl], S.[Farve ny], S.[L vand]
								,S.[Slut temp], S.[Vandpct]
                                FROM [dbo].[PRO_EXP_ORDER_UNLOAD_R] AS ULR
								LEFT JOIN SAMPLES AS S
									ON ULR.[ORDER_NAME] = S.[ORDER_NAME]
                                WHERE ULR.[ORDER_NAME] IN ({sql_roast_orders})
                                GROUP BY DATEADD(D, DATEDIFF(D, 0, ULR.[RECORDING_DATE] ), 0)
                                ,ULR.[DEST_NAME] ,ULR.[ORDER_NAME]
								,S.[Antal samples], S.[Farve gl], S.[Farve ny], S.[L vand]
								,S.[Slut temp], S.[Vandpct] """
    # Only try to read query if any orders exist
    if len(sql_roast_orders) > 0:
        df_probat_roast_output = pd.read_sql(query_probat_roast_output, sssi.con_probat)
    else:
        df_probat_roast_output = pd.DataFrame()

    # Read grinding orders form Probat
    query_probat_grinding_input = f""" SELECT [ORDER_NAME] AS [Ordrenummer]
                            	  ,[CUSTOMER_CODE] AS [Varenummer]
                            	  ,DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                            	  ,[DESTINATION] AS [Mølle]
                            	  FROM [dbo].[PRO_EXP_ORDER_LOAD_G]
                            	  WHERE [S_ORDER_NAME] IN ({sql_roast_orders})
                            	  GROUP BY [ORDER_NAME],[CUSTOMER_CODE]
                            	  ,DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
                            	  ,[DESTINATION] """
    # Only try to read query if any orders exist
    if len(sql_roast_orders) > 0:
        df_probat_grinding_input = pd.read_sql(query_probat_grinding_input, sssi.con_probat)
    else:
        df_probat_grinding_input = pd.DataFrame(columns=['Ordrenummer'])

    # Convert orders to string for use in grinder output query
    grinder_orders = df_probat_grinding_input['Ordrenummer'].unique().tolist() if len(df_probat_grinding_input) > 0 else ["\'\'"]
    sql_grinder_orders = ssf.string_to_sql(grinder_orders) if grinder_orders else "\'\'"

    # Get output from grinders
    query_probat_grinding_output = f""" WITH SAMPLES AS ( SELECT [ORDER_NAME] ,AVG([SIEVE_1] / 100.0) AS [Si 1]
                                    ,AVG([SIEVE_2] / 100.0) AS [Si 2] ,AVG([SIEVE_3] / 100.0) AS [Si 3]
                                    ,AVG([BUND] / 100.0) AS [Bund]
                                    FROM [dbo].[PRO_EXP_SAMPLE_GRINDER]
                                    WHERE [ORDER_NAME] IN ({sql_grinder_orders})
                                    	AND [PRO_EXPORT_GENERAL_ID] IN ( SELECT MAX([PRO_EXPORT_GENERAL_ID]) 
                                                                     FROM [dbo].[PRO_EXP_SAMPLE_GRINDER] GROUP BY [SAMPLE_ID])
                                    GROUP BY [ORDER_NAME] )
            						SELECT ULG.[ORDER_NAME] AS [Ordrenummer]
                                   ,SUM(ULG.[WEIGHT] / 1000.0) AS [Kilo]
                                   ,ULG.[DEST_NAME] AS [Silo]
								   ,S.[Si 1], S.[Si 2], S.[Si 3], S.[Bund]
                                   FROM [dbo].[PRO_EXP_ORDER_UNLOAD_G] AS ULG
								    LEFT JOIN SAMPLES AS S
									ON ULG.[ORDER_NAME] = S.[ORDER_NAME]
                                   WHERE ULG.[ORDER_NAME] IN ({sql_grinder_orders})
                                   GROUP BY ULG.[ORDER_NAME],ULG.[DEST_NAME]
								   ,S.[Si 1], S.[Si 2], S.[Si 3], S.[Bund] """
    # Only try to read query if any orders exist
    if len(sql_grinder_orders) > 0:
        df_probat_grinding_output = pd.read_sql(query_probat_grinding_output, sssi.con_probat)
    else:
        df_probat_grinding_output = pd.DataFrame()

    # Get order relations from Probat for finished goods if possible
    query_probat_orders = f""" IF '{req_modtagelse}' = 'None' -- Modtagelse ikke defineret
                          BEGIN
                          -- Formalet kaffe
                          SELECT PG.[ORDER_NAME] AS [Ordrenummer],PG.[S_ORDER_NAME] AS [Relateret ordre],'Probat formalet pakkelinje' AS [Kilde]
                          FROM [dbo].[PRO_EXP_ORDER_LOAD_R] AS LR
                          INNER JOIN [dbo].[PRO_EXP_ORDER_LOAD_G] AS LG
                        	ON LR.[ORDER_NAME] = LG.[S_ORDER_NAME]
                          INNER JOIN [dbo].[PRO_EXP_ORDER_SEND_PG] AS PG
                        	ON LG.[ORDER_NAME] = PG.[S_ORDER_NAME]
                          WHERE LR.[S_CONTRACT_NO] = '{req_reference_no}'
                        	AND PG.[ORDER_NAME] <> ''
                          GROUP BY PG.[ORDER_NAME],PG.[S_ORDER_NAME]                      	
                          UNION ALL
                          -- Helbønne
                          SELECT PB.[ORDER_NAME] AS [Ordrenummer],PB.[S_ORDER_NAME] AS [Relateret ordre],'Probat helbønne pakkelinje'
                          FROM [dbo].[PRO_EXP_ORDER_LOAD_R] AS LR
                          INNER JOIN [dbo].[PRO_EXP_ORDER_SEND_PB] AS PB
                        	ON LR.[ORDER_NAME] = PB.[S_ORDER_NAME]
                          WHERE LR.[S_CONTRACT_NO] = '{req_reference_no}'
                        	AND PB.[ORDER_NAME] <> ''
                          GROUP BY PB.[ORDER_NAME],PB.[S_ORDER_NAME]
                          UNION ALL
    					  -- Mølleordrer
                          SELECT LG.[ORDER_NAME] AS [Ordrenummer],LG.[S_ORDER_NAME] AS [Relateret ordre],'Probat mølle' AS [Kilde]
                          FROM [dbo].[PRO_EXP_ORDER_LOAD_R] AS LR
                          INNER JOIN [dbo].[PRO_EXP_ORDER_LOAD_G] AS LG
                        	ON LR.[ORDER_NAME] = LG.[S_ORDER_NAME]
                          WHERE LR.[S_CONTRACT_NO] = '{req_reference_no}'
                        	AND LG.[ORDER_NAME] <> ''
                          GROUP BY LG.[ORDER_NAME],LG.[S_ORDER_NAME]                      	
                          END
                          IF '{req_modtagelse}' <> 'None' -- Modtagelse defineret
                          BEGIN
                          -- Formalet kaffe
                          SELECT PG.[ORDER_NAME] AS [Ordrenummer],PG.[S_ORDER_NAME] AS [Relateret ordre],'Probat formalet pakkelinje' AS [Kilde]
                          FROM [dbo].[PRO_EXP_ORDER_LOAD_R] AS LR
                          INNER JOIN [dbo].[PRO_EXP_ORDER_LOAD_G] AS LG
                        	ON LR.[ORDER_NAME] = LG.[S_ORDER_NAME]
                          INNER JOIN [dbo].[PRO_EXP_ORDER_SEND_PG] AS PG
                        	ON LG.[ORDER_NAME] = PG.[S_ORDER_NAME]
                          WHERE LR.[S_CONTRACT_NO] = '{req_reference_no}'
                        	AND LR.[S_DELIVERY_NAME] = '{req_modtagelse}'
                        	AND PG.[ORDER_NAME] <> ''
                          GROUP BY PG.[ORDER_NAME],PG.[S_ORDER_NAME]
                          UNION ALL
                          -- Helbønne
                          SELECT PB.[ORDER_NAME] AS [Ordrenummer],PB.[S_ORDER_NAME] AS [Relateret ordre],'Probat helbønne pakkelinje'
                          FROM [dbo].[PRO_EXP_ORDER_LOAD_R] AS LR
                          INNER JOIN [dbo].[PRO_EXP_ORDER_SEND_PB] AS PB
                        	ON LR.[ORDER_NAME] = PB.[S_ORDER_NAME]
                          WHERE LR.[S_CONTRACT_NO] = '{req_reference_no}'
                        	AND LR.[S_DELIVERY_NAME] = '{req_modtagelse}'
                        	AND PB.[ORDER_NAME] <> ''
                          GROUP BY PB.[ORDER_NAME],PB.[S_ORDER_NAME]
                          UNION ALL
    					  -- Mølleordrer
                          SELECT LG.[ORDER_NAME] AS [Ordrenummer],LG.[S_ORDER_NAME] AS [Relateret ordre],'Probat mølle' AS [Kilde]
                          FROM [dbo].[PRO_EXP_ORDER_LOAD_R] AS LR
                          INNER JOIN [dbo].[PRO_EXP_ORDER_LOAD_G] AS LG
                        	ON LR.[ORDER_NAME] = LG.[S_ORDER_NAME]
                          WHERE LR.[S_CONTRACT_NO] = '{req_reference_no}'
                        	AND LR.[S_DELIVERY_NAME] = '{req_modtagelse}'
                        	AND LG.[ORDER_NAME] <> ''
                          GROUP BY LG.[ORDER_NAME],LG.[S_ORDER_NAME]
                          END """
    df_probat_orders = pd.read_sql(query_probat_orders, sssi.con_probat)
    df_probat_orders_top = df_probat_orders.loc[df_probat_orders['Kilde'] != 'Probat mølle']

    # Join previous found orders to one list for query below
    sql_related_orders = ssf.string_to_sql(roast_orders + grinder_orders)

    # Get related orders from Navision
    df_nav_order_related = ssf.get_nav_orders_from_related_orders(sql_related_orders)

    # Get list of orders and append to lists if they do not already exist
    # Merge Probat and NAV orders before merging
    nav_orders_top = df_nav_order_related['Ordrenummer'].unique().tolist()
    nav_orders_related = df_nav_order_related['Relateret ordre'].unique().tolist()
    probat_orders_top = df_probat_orders_top['Ordrenummer'].unique().tolist()
    probat_orders_related = df_probat_orders_top['Relateret ordre'].unique().tolist()

    # Create list dependent on request relationsship type, defined when report is requested by user
    orders_top_level = ssf.extend_order_list(req_ordrelationstype, orders_top_level, probat_orders_top, nav_orders_top)
    orders_related = ssf.extend_order_list(req_ordrelationstype, orders_related, probat_orders_related, nav_orders_related)
    # String used for querying Navision, only finished goods
    req_orders_total = ssf.string_to_sql(orders_top_level)

    # Get a string with all lotnumbers produced directly or indirectly using any of the identified orders
    nav_lotnots_total_sql_string = ssfg.get_nav_lotnos_from_orders(req_orders_total, 'string')
    # Get information about each production order based on lotnumbers identified above
    df_nav_færdigvaretilgang = ssfg.get_production_information(nav_lotnots_total_sql_string) if nav_lotnots_total_sql_string else pd.DataFrame(columns=['Produceret','Salg','Regulering & ompak','Restlager'])
    # Get information about any sales to any customers based on lotnumbers identified above
    df_nav_debitorer = ssfg.get_sales_information(nav_lotnots_total_sql_string) if nav_lotnots_total_sql_string else pd.DataFrame()
    # Get relationship between requested order and any orders which have used it as components, based on lotnumbers identified above
    df_nav_orders = ssfg.get_order_relationship(nav_lotnots_total_sql_string) if nav_lotnots_total_sql_string else pd.DataFrame()

    # Query to get karakterer saved in BKI_Datastore
    query_ds_karakterer = f""" IF '{req_modtagelse}' = 'None'
                          BEGIN
                          SELECT SK.[Id],RRP.[Id] AS [Risteri id],SK.[Bruger] AS [Person],SK.[Dato] AS [Registreringstidspunkt]
                          ,SK.[Smag_Syre] AS [Syre],SK.[Smag_Krop] AS [Krop],SK.[Smag_Aroma] AS [Aroma],SK.[Smag_Eftersmag] AS [Eftersmag]
                          ,SK.[Smag_Robusta] AS [Robusta],ISNULL(S.[Beskrivelse],'Ej smagt') AS [Status],SK.[Bemærkning]
                          FROM [cof].[Smageskema] AS SK
                          LEFT JOIN [cof].[Risteri_modtagelse_registrering] AS RMR
                        	ON SK.[Id_org] = RMR.[Id]
                            AND RMR.[Id_org_kildenummer] = 3
                          LEFT JOIN [cof].[Risteri_råkaffe_planlægning] AS RRP
                        	ON RMR.[Id_org] = RRP.[Id]
                          LEFT JOIN [cof].[Status] AS S
                        	ON SK.[Status] = S.[Id]
                          WHERE SK.[Kontraktnummer] = '{req_reference_no}'
                          END
                          IF '{req_modtagelse}' <> 'None'
                          BEGIN
                          SELECT SK.[Id],RRP.[Id] AS [Risteri id],SK.[Bruger] AS [Person],SK.[Dato] AS [Registreringstidspunkt]
                          ,SK.[Smag_Syre] AS [Syre],SK.[Smag_Krop] AS [Krop],SK.[Smag_Aroma] AS [Aroma],SK.[Smag_Eftersmag] AS [Eftersmag]
                          ,SK.[Smag_Robusta] AS [Robusta],ISNULL(S.[Beskrivelse],'Ej smagt') AS [Status],SK.[Bemærkning]
                          FROM [cof].[Smageskema] AS SK
                          LEFT JOIN [cof].[Risteri_modtagelse_registrering] AS RMR
                        	ON SK.[Id_org] = RMR.[Id]
                            AND RMR.[Id_org_kildenummer] = 3
                          LEFT JOIN [cof].[Risteri_råkaffe_planlægning] AS RRP
                        	ON RMR.[Id_org] = RRP.[Id]
                          LEFT JOIN [cof].[Status] AS S
                        	ON SK.[Status] = S.[Id]
                          WHERE SK.[Kontraktnummer] = '{req_reference_no}'
                        	AND RRP.[Delivery] = '{req_modtagelse}'
                          END """
    df_ds_karakterer = pd.read_sql(query_ds_karakterer, sssi.con_ds)

    # Samples for green coffees from Probat
    query_probat_gc_samples = f""" IF '{req_modtagelse}' = 'None'
                            BEGIN
                            SELECT [RECORDING_DATE] AS [Dato],[SAMPLE_ID] AS [Probat id],[VOLUME] AS [Volumen]
                            ,[HUMIDITY_1] / 10000.0 AS [Vandprocent 1],[HUMIDITY_2] / 10000.0 AS [Vandprocent 2]
                            ,[HUMIDITY_3] / 10000.0 AS [Vandprocent 3],[USERNAME] AS [Bruger],[INFO] AS [Bemærkning]
                            FROM [dbo].[PRO_EXP_SAMPLE_RECEIVING]
                            WHERE [PRO_EXPORT_GENERAL_ID] IN (SELECT MAX([PRO_EXPORT_GENERAL_ID]) FROM [dbo].[PRO_EXP_SAMPLE_RECEIVING] GROUP BY [SAMPLE_ID])
                            	AND [CONTRACT_NO] = '{req_reference_no}'
                            END
                            IF '{req_modtagelse}' <> 'None'
                            BEGIN
                            SELECT [RECORDING_DATE] AS [Dato],[SAMPLE_ID] AS [Probat id],[VOLUME] AS [Volumen]
                            ,[HUMIDITY_1] / 10000.0 AS [Vandprocent 1],[HUMIDITY_2] / 10000.0 AS [Vandprocent 2]
                            ,[HUMIDITY_3] / 10000.0 AS [Vandprocent 3]
                            ,[USERNAME] AS [Bruger],[INFO] AS [Bemærkning]
                            FROM [dbo].[PRO_EXP_SAMPLE_RECEIVING]
                            WHERE [PRO_EXPORT_GENERAL_ID] IN (SELECT MAX([PRO_EXPORT_GENERAL_ID]) FROM [dbo].[PRO_EXP_SAMPLE_RECEIVING] GROUP BY [SAMPLE_ID])
                            	AND [CONTRACT_NO] = '{req_reference_no}'
                                AND [DELIVERY_NAME] = '{req_modtagelse}'
                            END """
    df_probat_gc_samples = pd.read_sql(query_probat_gc_samples, sssi.con_probat)

    # =============================================================================
    # Section 1: Generelt
    # =============================================================================
    section_id = 1
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Kontraktnummer','Modtagelse','Varenummer','Varenavn','Mærkningsordning','Oprindelsesland',
                    'Leverandørnummer','Leverandørnavn','Godkendt på id','Silobeholdning eksporteret']

    if ssf.get_section_status_code(df_nav_generelt) == 99:
        try:
            df_nav_generelt['Kontraktnummer'] = req_reference_no
            df_nav_generelt['Modtagelse'] = req_modtagelse
            df_nav_generelt['Silobeholdning eksporteret'] = df_probat_inventory_timestamp['Silobeholdning eksporteret'].iloc[0]
            df_nav_generelt['Godkendt på id'] = ssf.get_contract_delivery_approval_id(req_reference_no, req_modtagelse)
            # Apply column formating
            df_nav_generelt['Silobeholdning eksporteret'] = df_nav_generelt['Silobeholdning eksporteret'].dt.strftime('%d-%m-%Y %H:%M')
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
    # Section 21: Modtagelse
    # =============================================================================
    section_id = 21
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Placering','Dato','Kilo','Restlager']
    columns_1_dec = ['Kilo','Restlager']

    if ssf.get_section_status_code(df_probat_receiving) == 99:
        try:
            # Create total for dataframe
            dict_modtagelse_total = {'Kilo': [df_probat_receiving['Kilo'].sum()],
                                     'Restlager': [df_probat_receiving['Restlager'].sum()]}
            # Create temp dataframe including total
            df_temp_total = pd.concat([df_probat_receiving,
                                       pd.DataFrame.from_dict(data=dict_modtagelse_total, orient = 'columns')])
            # Apply column formating
            df_temp_total['Dato'] = df_temp_total['Dato'].dt.strftime('%d-%m-%Y')
            for col in columns_1_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            df_temp_total = df_temp_total[column_order]
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_total, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_receiving))

    # =============================================================================
    # Section 20: Rensning
    # =============================================================================
    section_id = 20
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Silo','Dato','Sortnummer','Sortnavn','Kilo','Restlager']
    columns_1_dec = ['Kilo','Restlager']
    columns_strip = ['Dato']

    if ssf.get_section_status_code(df_probat_processing) == 99:
        try:
            # Apply column formating for date column before concat
            df_probat_processing['Dato'] = df_probat_processing['Dato'].dt.strftime('%d-%m-%Y')
            df_probat_processing.fillna('', inplace=True)
            #Concat dates into one strng and sum numeric columns if they can be grouped
            df_probat_processing = df_probat_processing.groupby(['Silo','Sortnummer'],dropna=False).agg(
                {'Kilo': 'sum',
                 'Restlager': 'sum',
                 'Dato': lambda x: ','.join(sorted(pd.Series.unique(x)))
                 }).reset_index()
            # Add item name
            df_probat_processing['Sortnavn'] = df_probat_processing['Sortnummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
            # Remove trailing and leading commas from strings
            for col in columns_strip:
                df_probat_processing[col] = df_probat_processing[col].apply(lambda x: ssf.strip_comma_from_string(x))
            # Create total for dataframe
            dict_modtagelse_total = {'Kilo': [df_probat_processing['Kilo'].sum()],
                                     'Restlager': [df_probat_processing['Restlager'].sum()]}
            # Create temp dataframe including total
            df_temp_total = pd.concat([df_probat_processing,
                                       pd.DataFrame.from_dict(data=dict_modtagelse_total, orient = 'columns')])
            for col in columns_1_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            df_temp_total = df_temp_total[column_order]
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_total, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_processing))

    # =============================================================================
    # Section 5: Risteordrer
    # =============================================================================
    section_id = 5
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Dato','Rister','Ordrenummer','Silo',
                    'Kilo råkaffe','Heraf kontrakt','Kilo ristet','Farve gl',
                    'Farve ny','Vandpct','Antal samples','L vand','Slut temp']
    columns_1_dec = ['Kilo råkaffe','Heraf kontrakt','Kilo ristet','Farve gl',
                     'Farve ny','L vand','Slut temp']
    columns_2_pct = ['Vandpct']
    columns_strip = ['Dato','Silo']

    if ssf.get_section_status_code(df_probat_roast_input) == 99:
        try:
            # Apply column formating for date column before concat
            df_probat_roast_output['Dato'] = df_probat_roast_output['Dato'].dt.strftime('%d-%m-%Y')
            # Concat dates into one strng and sum numeric columns if they can be grouped
            df_probat_roast_output = df_probat_roast_output.groupby(['Ordrenummer','Farve gl',
                                                                     'Farve ny','Vandpct','Antal samples',
                                                                     'L vand','Slut temp'],dropna=False).agg(
                {'Kilo ristet': 'sum',
                 'Dato': lambda x: ','.join(sorted(pd.Series.unique(x))),
                 'Silo': lambda x: ','.join(sorted(pd.Series.unique(x)))
                 }).reset_index()
            # Join roast output to input for one table
            df_probat_roast_total = pd.merge(df_probat_roast_input,
                                             df_probat_roast_output,
                                             left_on = 'Ordrenummer',
                                             right_on = 'Ordrenummer',
                                             how = 'left',
                                             suffixes = ('' ,'_R')
                                             )
            #Column formating and lookups
            for col in columns_strip:
                df_probat_roast_total[col] = df_probat_roast_total[col].apply(lambda x: ssf.strip_comma_from_string(x))
            df_probat_roast_total['Varenavn'] = df_probat_roast_total['Varenummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
            # Create total for dataframe
            dict_risteordrer_total = {'Kilo råkaffe': df_probat_roast_total['Kilo råkaffe'].sum(),
                                     'Heraf kontrakt': df_probat_roast_total['Heraf kontrakt'].sum(),
                                     'Kilo ristet': df_probat_roast_total['Kilo ristet'].sum()
                                     }
            # Create temp dataframe including total
            df_temp_total = pd.concat([df_probat_roast_total,
                                   pd.DataFrame([dict_risteordrer_total])])
            for col in columns_1_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            for col in columns_2_pct:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: ssf.number_format(x, 'pct_2'))
                df_temp_total.replace({'nan': None, 'nan%': None}, inplace=True)
            df_temp_total = df_temp_total[column_order]
            df_temp_total.sort_values(by=['Varenummer'] ,inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_total, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_roast_output))

    # =============================================================================
    # Section 4: Mølleordrer
    # =============================================================================
    section_id = 4
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Ordrenummer','Dato','Silo','Kilo',
                    'Si 1','Si 2','Si 3','Bund']
    columns_1_dec = ['Kilo','Si 1', 'Si 2', 'Si 3', 'Bund']
    columns_strip = ['Dato','Silo']

    if ssf.get_section_status_code(df_probat_grinding_input) == 99:
        try:
            # Apply column formating for date column before concat
            df_probat_grinding_input['Dato'] = df_probat_grinding_input['Dato'].dt.strftime('%d-%m-%Y')
            # Concat dates into one string and sum numeric columns if they can be grouped
            df_probat_grinding_input = df_probat_grinding_input.groupby(['Ordrenummer','Varenummer','Mølle']).agg(
                {'Dato': lambda x: ','.join(sorted(pd.Series.unique(x)))
                }).reset_index()
            df_probat_grinding_output = df_probat_grinding_output.groupby(['Ordrenummer','Si 1','Si 2',
                                                                           'Si 3','Bund'],dropna=False).agg(
                {'Kilo': 'sum',
                 'Silo': lambda x: ','.join(sorted(pd.Series.unique(x)))
                }).reset_index()
            # Join roast output to input for one table
            df_probat_grinding_total = pd.merge(df_probat_grinding_input,
                                             df_probat_grinding_output,
                                             left_on = 'Ordrenummer',
                                             right_on = 'Ordrenummer',
                                             how = 'left',
                                             suffixes = ('' ,'_R')
                                             )
            #Column formating and lookups
            for col in columns_strip:
                df_probat_grinding_total[col] = df_probat_grinding_total[col].apply(lambda x: ssf.strip_comma_from_string(x))
            df_probat_grinding_total['Varenavn'] = df_probat_grinding_total['Varenummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
            # Create total for dataframe
            dict_mølleordrer_total = {'Kilo': df_probat_grinding_total['Kilo'].sum()}
            # Create temp dataframe including total
            df_temp_total = pd.concat([df_probat_grinding_total,
                                   pd.DataFrame([dict_mølleordrer_total])])
            for col in columns_1_dec:
                df_temp_total[col] = df_temp_total[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            df_temp_total.replace({'nan': None}, inplace=True)
            df_temp_total = df_temp_total[column_order]
            df_temp_total.sort_values(by=['Varenummer'] ,inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_total, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_roast_output))

    # =============================================================================
    # Section 3: Færdigvaretilgang
    # =============================================================================
    section_id = 3
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Ordrenummer','Udløbsdato','Produceret','Salg','Restlager','Regulering & ompak']
    columns_1_dec = ['Produceret','Salg','Restlager','Regulering & ompak']
    columns_strip = ['Ordrenummer','Udløbsdato']

    if ssf.get_section_status_code(df_nav_færdigvaretilgang) == 99:
        try:
            df_nav_færdigvaretilgang['Udløbsdato'] = df_nav_færdigvaretilgang['Udløbsdato'].dt.strftime('%d-%m-%Y')
            # Concat order numbers to one string
            df_nav_færdigvaretilgang = df_nav_færdigvaretilgang.groupby(['Varenummer','Varenavn']).agg(
                {'Ordrenummer': lambda x: ','.join(sorted(pd.Series.unique(x))),
                 'Udløbsdato': lambda x: ','.join(sorted(pd.Series.unique(x))),
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
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_temp_total))

    # =============================================================================
    # Section 8: Massebalance
    # =============================================================================
    section_id = 8
    section_name = ssf.get_section_name(section_id, df_sections)
    columns_1_dec = ['[1] Kontrakt','[2] Renset','[3] Restlager','[4] Difference','[6] Anvendt til produktion',
                     '[7] Difference','[9] Ristet kaffe','[10] Difference','[12] Færdigvareproduktion',
                     '[13] Difference','[15] Salg','[16] Regulering & ompak','[17] Restlager','[18] Difference']
    columns_2_pct = ['[5] Difference pct','[8] Difference pct','[11] Difference pct','[14] Difference pct','[19] Difference pct']

    dict_massebalance = {'[1] Kontrakt': df_probat_receiving['Kilo'].sum(),
                         '[2] Renset': df_probat_processing['Kilo'].sum(),
                         '[3] Restlager': df_probat_processing['Restlager'].sum(),
                         '[4] Difference': None,
                         '[5] Difference pct': None,
                         '[6] Anvendt til produktion': df_probat_roast_total['Heraf kontrakt'].sum(),
                         '[7] Difference': None,
                         '[8] Difference pct': None,
                         '[9] Ristet kaffe': df_probat_roast_total['Kilo ristet'].sum(),
                         '[10] Difference': None,
                         '[11] Difference pct': None,
                         '[12] Færdigvareproduktion': df_nav_færdigvaretilgang['Produceret'].sum(),
                         '[13] Difference': None,
                         '[14] Difference pct': None,
                         '[15] Salg': df_nav_færdigvaretilgang['Salg'].sum(),
                         '[16] Regulering & ompak': df_nav_færdigvaretilgang['Regulering & ompak'].sum(),
                         '[17] Restlager': df_nav_færdigvaretilgang['Restlager'].sum(),
                         '[18] Difference': None,
                         '[19] Difference pct': None
                        }
    # Calculate differences and percentages before converting to dataframe:
    dict_massebalance['[4] Difference'] = dict_massebalance['[1] Kontrakt'] - dict_massebalance['[2] Renset'] - dict_massebalance['[3] Restlager']
    dict_massebalance['[5] Difference pct'] = ssf.zero_division(dict_massebalance['[4] Difference'], dict_massebalance['[1] Kontrakt'], 'None')
    dict_massebalance['[7] Difference'] = ( dict_massebalance['[2] Renset'] - dict_massebalance['[3] Restlager']
                                            - dict_massebalance['[6] Anvendt til produktion'] )
    dict_massebalance['[8] Difference pct'] = ssf.zero_division(dict_massebalance['[7] Difference'],
                                                            dict_massebalance['[2] Renset'] - dict_massebalance['[3] Restlager'], 'None')
    dict_massebalance['[10] Difference'] = dict_massebalance['[2] Renset'] - dict_massebalance['[3] Restlager'] - dict_massebalance['[9] Ristet kaffe']
    dict_massebalance['[11] Difference pct'] = ssf.zero_division(dict_massebalance['[10] Difference'],
                                                            dict_massebalance['[2] Renset'] - dict_massebalance['[3] Restlager'], 'None')
    dict_massebalance['[13] Difference'] = dict_massebalance['[9] Ristet kaffe'] - dict_massebalance['[12] Færdigvareproduktion']
    dict_massebalance['[14] Difference pct'] = ssf.zero_division(dict_massebalance['[13] Difference'], dict_massebalance['[12] Færdigvareproduktion'], 'None')
    dict_massebalance['[18] Difference'] = ( dict_massebalance['[12] Færdigvareproduktion'] - dict_massebalance['[15] Salg']
                                             - dict_massebalance['[16] Regulering & ompak'] - dict_massebalance['[17] Restlager'] )
    dict_massebalance['[19] Difference pct'] = ssf.zero_division(dict_massebalance['[18] Difference'],
                                                             dict_massebalance['[12] Færdigvareproduktion'], 'None')
    #Number formating
    for col in columns_1_dec:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'dec_1')
    for col in columns_2_pct:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'pct_2')

    df_massebalance = pd.DataFrame.from_dict(data=dict_massebalance, orient='index').reset_index()
    df_massebalance.columns = ['Sektion','Værdi']
    df_massebalance['Note'] = [None,None,None,'[1]-[2]-[3]','[4]/[1]',None,'[2]-[3]-[6]','[7]/([2]-[3])',None,
                               '[2]-[3]-[9]','[10]/([2]-[3])',None,'[9]-[12]','[13]/[12]',None,None,None,
                               '[12]-[15]-[16]-[17]','[18]/[12]']
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
    # Section 2: Relaterede ordrer Kontrakt --> færdigvare
    # =============================================================================
    section_id = 2
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Ordrenummer','Varenummer','Navn','Relateret ordre',
                    'Relateret vare','Relateret navn','Kilde']

    if req_ordrelationstype == 0:
        df_temp_orders = pd.concat([df_nav_orders,df_probat_orders,df_nav_order_related])
    elif req_ordrelationstype == 1:
        df_temp_orders = pd.concat([df_nav_orders,df_probat_orders])
    elif req_ordrelationstype == 2:
        df_temp_orders = pd.concat([df_nav_orders,df_nav_order_related
                                    ,df_probat_orders.loc[df_probat_orders['Kilde'] == 'Probat mølle']]) # Only Probat orders which are not related to finished goods

    if ssf.get_section_status_code(df_temp_orders) == 99:
        try:
            df_temp_orders['Varenummer'] = df_temp_orders['Ordrenummer'].apply(lambda x: ssf.get_nav_order_info(x))
            df_temp_orders['Navn'] = df_temp_orders['Varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_temp_orders['Relateret vare'] = df_temp_orders['Relateret ordre'].apply(lambda x: ssf.get_nav_order_info(x))
            df_temp_orders['Relateret navn'] = df_temp_orders['Relateret vare'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            # Remove orders not existing in NAV and sort columns and rows
            df_temp_orders.dropna(inplace=True)
            df_temp_orders = df_temp_orders[column_order]
            df_temp_orders.sort_values(by=['Ordrenummer','Relateret ordre'], inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_orders, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
    # =================================================================
    # Section 19: Relation visualization
    # =================================================================
            #Try to create .png with relations illustrated and add to .docx as well
            try:
                df_temp_order_relation = df_temp_orders[['Ordrenummer','Varenummer','Relateret ordre','Relateret vare']]
                df_temp_order_relation['Ordretype'] = df_temp_order_relation['Varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Varetype'))
                df_temp_order_relation['Relateret ordretype'] = df_temp_order_relation['Relateret vare'].apply(lambda x: ssf.get_nav_item_info(x, 'Varetype'))
                df_temp_order_relation['Primær'] = df_temp_order_relation['Ordretype'] + '\n' + df_temp_order_relation['Ordrenummer']
                df_temp_order_relation['Sekundær'] = df_temp_order_relation['Relateret ordretype'] + '\n' + df_temp_order_relation['Relateret ordre']
                df_temp_order_relation = df_temp_order_relation[['Primær','Sekundær']]
                # Add green coffees
                df_temp_gc_orders = pd.DataFrame(columns=['Primær','Sekundær'])
                df_temp_gc_orders['Primær'] = 'Ristet kaffe' + '\n' + df_probat_roast_input['Ordrenummer']
                df_temp_gc_orders['Sekundær'] = 'Råkaffe' + '\n' + req_reference_no
                df_order_relations = pd.concat([df_temp_order_relation,df_temp_gc_orders])
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
        ssf.create_image_from_binary_string(path_png_relations)
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_temp_orders))

    # =============================================================================
    # Section 12: Karakterer
    # =============================================================================
    section_id = 12
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Id','Risteri id','Person','Registreringstidspunkt','Syre','Krop','Aroma','Eftersmag','Robusta','Status','Bemærkning']
    columns_1_dec = ['Syre','Krop','Aroma','Eftersmag','Robusta']

    if ssf.get_section_status_code(df_ds_karakterer) == 99:
        try:
            # String format datecolumn for export and numeric formating
            for col in columns_1_dec:
                df_ds_karakterer[col] = df_ds_karakterer[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            df_ds_karakterer['Registreringstidspunkt'] = df_ds_karakterer['Registreringstidspunkt'].dt.strftime('%d-%m-%Y')
            df_ds_karakterer.sort_values(by=['Id'], inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_ds_karakterer, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_ds_karakterer))

    # =============================================================================
    # Section 22: Probat samples
    # =============================================================================
    section_id = 22
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Dato','Probat id','Volumen,Vandprocent 1','Vandprocent 2','Vandprocent 3'
                    ,'Bruger','Bemærkning']
    columns_2_pct = ['Vandprocent 1','Vandprocent 2','Vandprocent 3']

    if ssf.get_section_status_code(df_probat_gc_samples) == 99:
        try:
            # String format datecolumn for export
            for col in columns_2_pct:
                df_probat_gc_samples[col] = df_probat_gc_samples[col].apply(lambda x: ssf.number_format(x, 'pct_2'))
            df_probat_gc_samples['Dato'] = df_probat_gc_samples['Dato'].dt.strftime('%d-%m-%Y')
            df_probat_gc_samples.sort_values(by=['Probat id'], inplace=True)
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_probat_gc_samples, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_gc_samples))

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
    pd.DataFrame(data=dict_email_log, index=[0]).to_sql('Sporbarhed_email_log', con=sssi.con_ds, schema='trc', if_exists='append', index=False)
    ssf.log_insert(script_name, f'Request id: {req_id} inserted into [trc].[Email_log]')

    # =============================================================================
    # Update request that dataprocessing has been completed
    # =============================================================================
    sssi.con_ds.execute(f"""UPDATE [trc].[Sporbarhed_forespørgsel]
                      SET Data_færdigbehandlet = 1
                      WHERE [Id] = {req_id}""")
    ssf.log_insert(script_name, f'Request id: {req_id} completed')

    # Exit script
    ssf.get_exit_check(0)
