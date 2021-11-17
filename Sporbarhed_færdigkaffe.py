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
    con_probat = ssf.get_connection('probat')
    con_comscale = ssf.get_connection('comscale')

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

    script_name = 'Sporbarhed_færdigkaffe.py'
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

    production_machine = df_results_generelt['Pakkelinje'].iloc[0]

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
                       WHERE KP.[Ordrenummer] = '{req_reference_no}' """
    df_prøver = pd.read_sql(query_ds_samples, con_ds)

    # All grades given for the requested order. Coalesce is to ensure that query
    # returns no results if record exists but no grades have been given
    query_ds_karakterer = f""" SELECT [Id] ,[Dato] ,[Bruger] ,[Smag_Syre] AS [Syre]
                          ,[Smag_Krop] AS [Krop] ,[Smag_Aroma] AS [Aroma] 
                          ,[Smag_Eftersmag] AS [Eftersmag],[Smag_Robusta] AS [Robusta] ,[Bemærkning]
                          FROM [cof].[Smageskema]
                          WHERE [Referencetype] = 2	
                              AND [Referencenummer] = '{req_reference_no}'
                              AND COALESCE([Smag_Syre],[Smag_Krop],[Smag_Aroma],
                                [Smag_Eftersmag],[Smag_Robusta]) IS NOT NULL"""
    df_karakterer = pd.read_sql(query_ds_karakterer, con_ds)

    # If lotnumbers from requested order have been checked for leakage the information
    # from the check is returned with this query. Will often return no results
    query_ds_vacslip = """ SELECT [Registreringstidspunkt] AS [Kontroltidspunkt]
                       ,[Initialer] AS [Kontrolleret af],[Lotnummer]
                       ,[Pallenummer],[Antal_poser] AS [Antal leakers]
                       ,[Bemærkning] AS [Kontrol bemærkning]
    				   ,CASE WHEN [Overført_email_log] = 1 THEN
    				   'Over grænseværdi' ELSE 'Ok' END AS [Resultat af kontrol]
                       FROM [cof].[Vac_slip] """
    df_ds_vacslip = pd.read_sql(query_ds_vacslip, con_ds)

    # Primary packaging material - valve for bag
    query_ds_ventil = f""" SELECT [Varenummer] ,[Batchnr_stregkode] AS [Lotnummer]
                      FROM [cof].[Ventil_registrering]
                      WHERE [Ordrenummer] = '{req_reference_no}' """
    df_ds_ventil = pd.read_sql(query_ds_ventil, con_ds)

    # Order statistics from Comscale. Only for good bags (trade)
    query_com_statistics = f""" WITH CTE AS ( SELECT SD.[Nominal] ,SD.[Tare]
                           ,SUM( SD.[MeanValueTrade] * SD.[CounterGoodTrade] ) AS [Total vægt]
                           ,SUM( SD.[StandardDeviationTrade] * SD.[CounterGoodTrade] ) AS [Std afv]
                           ,SUM( SD.[CounterGoodTrade] ) AS [Antal poser]
                           FROM [ComScaleDB].[dbo].[StatisticData] AS SD
                           INNER JOIN [dbo].[Statistic] AS S ON SD.[Statistic_ID] = S.[ID]
                           WHERE S.[Order] = '{req_reference_no}' AND lower(S.[ArticleNumber]) NOT LIKE '%k'
                           GROUP BY S.[Order],SD.[Nominal],SD.[Tare] )
                           SELECT CTE.[Total vægt] / 1000.0 AS [Total vægt kg],CTE.[Antal poser]
                           ,CASE WHEN CTE.[Antal poser] = 0 
                           THEN NULL ELSE CTE.[Total vægt] / CTE.[Antal poser] END AS [Middelvægt g]
                           ,CASE WHEN CTE.[Antal poser] = 0 
                           THEN NULL ELSE CTE.[Std afv] / CTE.[Antal poser] END AS [Standardafvigelse g]
                           ,CASE WHEN CTE.[Antal poser] = 0 
                           THEN NULL ELSE CTE.[Total vægt] / CTE.[Antal poser] END - CTE.[Nominal] AS [Gns. godvægt per enhed g]
                           ,CTE.[Total vægt] - CTE.[Nominal] * CTE.[Antal poser] AS [Godvægt total g]
                           ,CTE.[Nominal] AS [Nominel vægt g],CTE.[Tare] AS [Taravægt g]
                           FROM CTE """
    df_com_statistics = pd.read_sql(query_com_statistics, con_comscale)

    # Query to pull various information from Navision for the requested order.
    query_nav_generelt = f""" WITH [RECEPT] AS (
                         SELECT	POC.[Prod_ Order No_],I.[No_]
                         FROM [dbo].[BKI foods a_s$Prod_ Order Component] (NOLOCK) AS POC
                         INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
                        	ON POC.[Item No_] = I.[No_]
                         WHERE [POC].[Prod_ Order Line No_] = 10000
                        	AND I.[Sequence Code] = 4)
                         ,[ILE] AS ( SELECT [Order No_],MIN([Posting Date]) AS [Posting Date]
                         ,SUM(CASE WHEN [Entry Type] = 5 AND [Location Code] = 'REWORK' 
                              THEN [Quantity] ELSE 0 END) AS [Rework forbrug]
                         ,SUM(CASE WHEN [Entry Type] = 6 AND [Location Code] = 'REWORK' 
                              THEN [Quantity] ELSE 0 END) AS [Rework afgang]
                         ,SUM(CASE WHEN [Entry Type] = 5 AND [Location Code] = 'SLAT' 
                              THEN [Quantity] ELSE 0 END) AS [Slat forbrug]
                         ,SUM(CASE WHEN [Entry Type] = 6 AND [Location Code] = 'SLAT' 
                              THEN [Quantity] ELSE 0 END) AS [Slat afgang]
                         FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK)
                         WHERE [Order Type] = 1 GROUP BY [Order No_] )
                         SELECT PO.[Source No_] AS [Varenummer]
                         ,I.[Description] AS [Varenavn]
                         ,I.[Base Unit of Measure] AS [Basisenhed]
                         ,CASE WHEN PO.[Status] = 0 THEN 'Simuleret'
                         WHEN PO.[Status] = 1 THEN 'Planlagt'
                         WHEN PO.[Status] = 2 THEN 'Fastlagt'
                         WHEN PO.[Status] = 3 THEN 'Frigivet'
                         WHEN PO.[Status] = 4 THEN 'Færdig'
                         END AS [Prod.ordre status]
                         ,ICR.[Cross-Reference No_] AS [Stregkode]
                         ,RECEPT.[No_] AS [Receptnummer],ILE.[Rework afgang]
                         ,ILE.[Posting Date] AS [Produktionsdato]
                         ,ILE.[Rework forbrug],ILE.[Slat afgang],ILE.[Slat forbrug]
                         FROM [dbo].[BKI foods a_s$Production Order] (NOLOCK) AS PO
                         INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
                        	ON PO.[Source No_] = I.[No_]
                         LEFT JOIN [dbo].[BKI foods a_s$Item Cross Reference] (NOLOCK) AS ICR
                        	ON I.[No_] = ICR.[Item No_] AND ICR.[Unit of Measure] = 'PS'
                        	AND ICR.[Cross-Reference Type] = 3
                         LEFT JOIN [RECEPT] ON PO.[No_] = RECEPT.[Prod_ Order No_]
                         LEFT JOIN [ILE] ON PO.[No_] = ILE.[Order No_]
                         WHERE I.[Item Category Code] = 'FÆR KAFFE' AND PO.[No_] = '{req_reference_no}' """
    df_nav_generelt = pd.read_sql(query_nav_generelt, con_nav)

    if len(df_nav_generelt) == 0:
        production_date = ''
    else:
        production_date = df_nav_generelt['Produktionsdato'].iloc[0]

    # Control of scales in packing area, 3 days back and 1 day ahead of production date
    query_ds_vægtkontrol = f""" SELECT V.[Registreringstidspunkt]
                           ,V.[Registreret_af] AS [Registreret af],V.[Vægt],V.[Serienummer]
                           ,CASE WHEN V.[Status] = 1 THEN 'Ok' ELSE 'Ej ok' END AS [Status]
                           FROM [cof].[Vægtkontrol] AS V
                           INNER JOIN [cof].[Serienummer_pakkelinje] AS SP
                           ON V.[Serienummer] = SP.[Serienummer]
                           WHERE SP.[Pakkelinje] = '{production_machine}'
                           AND DATEADD(d, DATEDIFF(d, 0, V.[Registreringstidspunkt] ), 0) 
                           BETWEEN DATEADD(d,-3, '{production_date}') AND DATEADD(d, 1, '{production_date}') """
    df_ds_vægtkontrol = pd.read_sql(query_ds_vægtkontrol, con_ds)

    # Get any related orders identified through Probat
    # Pakkelinjer is used to find either grinding or roasting orders used directly in packaging
    # Mølleordrer is used to find roasting orders used for grinding orders
    query_probat_orders = f""" WITH [CTE_ORDERS_PACK] AS (
                           SELECT [ORDER_NAME] AS [Ordrenummer],[S_ORDER_NAME] AS [Relateret ordre]
                           ,'Probat formalet pakkelinje' AS [Kilde]
                           FROM [dbo].[PRO_EXP_ORDER_SEND_PG]
                           GROUP BY	[ORDER_NAME],[S_ORDER_NAME]
                           UNION ALL
                           SELECT [ORDER_NAME],[S_ORDER_NAME],'Probat helbønne pakkelinje'
                           FROM [dbo].[PRO_EXP_ORDER_SEND_PB]
                           GROUP BY	[ORDER_NAME],[S_ORDER_NAME] )
    					   ,[CTE_ORDERS] AS (
                           SELECT [Ordrenummer],[Relateret ordre],[Kilde]
                           FROM [CTE_ORDERS_PACK]
                           WHERE [Relateret ordre] IN (SELECT [Relateret ordre] 
                           FROM [CTE_ORDERS_PACK] WHERE [Ordrenummer] = '{req_reference_no}'))
    					   SELECT * 
    					   FROM [CTE_ORDERS]
                           WHERE [Relateret ordre] <> 'Retour Ground'
    					   UNION ALL
    					   SELECT [ORDER_NAME] AS [Ordrenummer]
    					   ,[S_ORDER_NAME] AS [Relateret ordre]
    					   ,'Probat mølle' AS [Kilde]
    					   FROM [dbo].[PRO_EXP_ORDER_LOAD_G]
    					   WHERE [ORDER_NAME] IN (SELECT [Relateret ordre] FROM [CTE_ORDERS])
                           AND [S_ORDER_NAME] <> 'REWORK ROAST'
    					   GROUP BY [S_ORDER_NAME],[ORDER_NAME] """
    df_probat_orders = pd.read_sql(query_probat_orders, con_probat)

    # Get lists of orders and related orders (if any) from Probat, first create dataframe with top level orders:
    df_temp_top_level = df_probat_orders.loc[df_probat_orders['Kilde'] != 'Probat mølle']
    probat_orders_top = df_temp_top_level['Ordrenummer'].unique().tolist()
    probat_orders_related = df_probat_orders['Relateret ordre'].unique().tolist()

    # Get related orders from Navision
    query_nav_order_related = f"""WITH [CTE_ORDER] AS (SELECT [Prod_ Order No_]
                       ,[Reserved Prod_ Order No_]
                       FROM [dbo].[BKI foods a_s$Reserved Prod_ Order No_]
                       WHERE [Prod_ Order No_] = '{req_reference_no}' 
                       AND [Invalid] = 0)
                       SELECT [Prod_ Order No_] AS [Ordrenummer] 
                       ,[Reserved Prod_ Order No_] AS [Relateret ordre]
                       ,'Navision reservationer' AS [Kilde]
                       FROM [dbo].[BKI foods a_s$Reserved Prod_ Order No_]
                       WHERE [Reserved Prod_ Order No_] IN 
                       (SELECT [Reserved Prod_ Order No_] FROM [CTE_ORDER] )
                       AND [Invalid] = 0 """
    df_nav_order_related = pd.read_sql(query_nav_order_related, con_nav)

    # Get list of orders and append to lists if they do not already exist
    # Merge Probat and NAV orders before merging
    nav_orders_top = df_nav_order_related['Ordrenummer'].unique().tolist()
    nav_orders_related = df_nav_order_related['Relateret ordre'].unique().tolist()

    # Create list dependent on request relationsship type, defined when report is requested by user
    orders_top_level = ssf.extend_order_list(req_ordrelationstype, orders_top_level, probat_orders_top, nav_orders_top)
    orders_related = ssf.extend_order_list(req_ordrelationstype, orders_related, probat_orders_related, nav_orders_related)

    # String used for querying Navision, only finished goods
    req_orders_total = ssf.string_to_sql(orders_top_level)
    # String used for querying Probat for relation between grinder and roaster for visualization
    req_orders_related = ssf.string_to_sql(orders_related)

    # Get Probat relation between grinder and roaster for visualization
    query_probat_lg_to_ulr = f""" SELECT [ORDER_NAME] AS [Ordrenummer]
                                  ,[S_ORDER_NAME] AS [Relateret ordre] ,'Probat' AS [Kilde]
                                  FROM [dbo].[PRO_EXP_ORDER_LOAD_G]
                                  WHERE [S_ORDER_NAME] <> 'REWORK ROAST'
                                	AND [ORDER_NAME] IN ({req_orders_related})
                                  GROUP BY [ORDER_NAME],[S_ORDER_NAME] """
    if len(req_orders_related) != 0:
        df_probat_lg_to_ulr = pd.read_sql(query_probat_lg_to_ulr, con_probat)
    else:
        df_probat_lg_to_ulr = pd.DataFrame()

    # Get a string with all lotnumbers produced directly or indirectly using any of the identified orders
    nav_lotnots_total_sql_string = ssf.finished_goods.get_nav_lotnos_from_orders(req_orders_total, 'string')
    # Get information about each production order based on lotnumbers identified above
    df_nav_færdigvaretilgang = ssf.finished_goods.get_production_information(nav_lotnots_total_sql_string)
    # Get information about any sales to any customers based on lotnumbers identified above
    df_nav_debitorer = ssf.finished_goods.get_sales_information(nav_lotnots_total_sql_string)
    # Get relationship between requested order and any orders which have used it as components, based on lotnumbers identified above
    df_nav_orders = ssf.finished_goods.get_order_relationship(nav_lotnots_total_sql_string)

    # Lotnumber information for the originally requested order
    query_nav_lotno = f""" SELECT ILE.[Lot No_] AS [Lotnummer]
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
                          AND ILE.[Order No_] = '{req_reference_no}' """
    df_nav_lotno = pd.read_sql(query_nav_lotno, con_nav)

    # Primary packaging components used for the originally requested order
    query_nav_components = f""" SELECT POC.[Item No_] AS [Varenummer]
                    	   ,I.[Description] AS [Varenavn]
                           ,POAC.[Purchase Order No_] AS [Købsordre]
                           ,POAC.[Roll No_] AS [Rullenummer]
                           ,CAST(POAC.[Roll Lenght] AS INT) AS [Rullelængde]
                           ,POAC.[Batch_Lot No_] AS [Lotnummer]
                           ,POAC.[Packaging Date] AS [Pakkedato]
                           FROM [dbo].[BKI foods a_s$Prod_ Order Add_ Comp_] (NOLOCK) AS POAC
                           INNER JOIN [dbo].[BKI foods a_s$Prod_ Order Component] (NOLOCK) AS POC
                               ON POAC.[Prod_ Order No_] = POC.[Prod_ Order No_]
                               AND POAC.[Prod_ Order Line No_] = POC.[Prod_ Order Line No_]
                               AND POAC.[Prod_ Order Component Line No_] = POC.[Line No_]
                           INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
                               ON POC.[Item No_] = I.[No_]
                           WHERE POAC.[Prod_ Order No_] = '{req_reference_no}' """
    df_nav_components = pd.read_sql(query_nav_components, con_nav)

    # Components used for the originally requested order
    query_nav_consumption = f""" SELECT	ILE.[Item No_] AS [Varenummer]
                        	,I.[Description] AS [Varenavn]
                            ,I.[Base Unit of Measure] AS [Basisenhed]
                            ,SUM(ILE.[Quantity]) * -1 AS [Antal]
                            FROM [dbo].[BKI foods a_s$Item Ledger Entry] (NOLOCK) AS ILE
                            INNER JOIN [dbo].[BKI foods a_s$Item] (NOLOCK) AS I
                            	ON ILE.[Item No_] = I.[No_]
                            WHERE ILE.[Order No_] = '{req_reference_no}'
                            	AND ILE.[Entry Type] = 5
                            GROUP BY ILE.[Item No_] ,I.[Description],I.[Base Unit of Measure] """
    df_nav_consumption = pd.read_sql(query_nav_consumption, con_nav)

    q_related_orders = ssf.string_to_sql(orders_related)

    # Related grinding orders - information for batches out of grinder to include rework
    query_probat_ulg = f""" SELECT DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                       ,[PRODUCTION_ORDER_ID] AS [Probat id] ,[SOURCE_NAME] AS [Mølle]
                       ,[ORDER_NAME] AS [Ordrenummer] ,[D_CUSTOMER_CODE] AS [Receptnummer]
                       ,[DEST_NAME] AS [Silo],SUM([WEIGHT]) / 1000.0 AS [Kilo]
                       FROM [dbo].[PRO_EXP_ORDER_UNLOAD_G]
                       WHERE [ORDER_NAME] IN ({q_related_orders})
                       GROUP BY [PRODUCTION_ORDER_ID],[ORDER_NAME],[DEST_NAME],[SOURCE_NAME]
                       ,[D_CUSTOMER_CODE], DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) """
    df_probat_ulg = pd.DataFrame(columns=['Dato','Probat id','Mølle','Ordrenummer',
                                          'Receptnummer','Silo','Kilo'])
    if len(q_related_orders) != 0:
        df_probat_ulg = pd.read_sql(query_probat_ulg, con_probat)

    # Find related roasting orders from any related grinding orders
    query_probat_lg = f""" SELECT [S_ORDER_NAME]
                           FROM [dbo].[PRO_EXP_ORDER_LOAD_G]
                           WHERE [ORDER_NAME] IN ({q_related_orders}) 
                           AND [S_ORDER_NAME] <> 'REWORK ROAST'
                           GROUP BY	[S_ORDER_NAME] """
    df_probat_lg = pd.DataFrame(columns=['S_ORDER_NAME'])
    if len(q_related_orders) != 0:
        df_probat_lg = pd.read_sql(query_probat_lg, con_probat)

    if len(df_probat_ulg) != 0: # Add to list only if dataframe is not empty
        for order in df_probat_lg['S_ORDER_NAME'].unique().tolist():
            if order not in orders_related:
                orders_related.append(order)

    q_related_orders = ssf.string_to_sql(orders_related)

    # Find information for identified roasting orders, batches out of roaster
    query_probat_ulr = f""" SELECT [S_CUSTOMER_CODE] AS [Receptnummer]
                            ,DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0) AS [Dato]
                            ,[SOURCE_NAME] AS [Rister] ,[PRODUCTION_ORDER_ID] AS [Probat id]
                        	,[ORDER_NAME] AS [Ordrenummer] ,SUM([WEIGHT]) / 1000.0 AS [Kilo]
    						,[DEST_NAME] AS [Silo]
                            FROM [dbo].[PRO_EXP_ORDER_UNLOAD_R]
                            WHERE [ORDER_NAME] IN ({q_related_orders})
                            GROUP BY [S_CUSTOMER_CODE],[SOURCE_NAME],[PRODUCTION_ORDER_ID]
                            ,[ORDER_NAME],DATEADD(D, DATEDIFF(D, 0, [RECORDING_DATE] ), 0)
    						,[DEST_NAME] """
    df_probat_ulr = pd.DataFrame(columns=['Receptnummer','Dato','Rister','Probat id',
                                          'Ordrenummer','Kilo','Silo'])
    if len(q_related_orders) != 0:
        df_probat_ulr = pd.read_sql(query_probat_ulr, con_probat)

    # Find green coffee related to orders
    query_probat_lr = f""" SELECT [S_TYPE_CELL] AS [Sortnummer] ,[Source] AS [Silo]
                    ,[S_CONTRACT_NO] AS [Kontraktnummer]
                    ,[S_DELIVERY_NAME] AS [Modtagelse],[ORDER_NAME] AS [Ordrenummer]
                	,SUM([WEIGHT]) / 1000.0 AS [Kilo]
                    FROM [dbo].[PRO_EXP_ORDER_LOAD_R]
                    WHERE [ORDER_NAME] IN ({q_related_orders})
                    GROUP BY [S_TYPE_CELL],[Source],[S_CONTRACT_NO]
                    	,[S_DELIVERY_NAME],[ORDER_NAME] """
    df_probat_lr = pd.DataFrame(columns=['Sortnummer','Silo','Kontraktnummer',
                                         'Modtagelse','Ordrenummer','Kilo'])
    if len(q_related_orders) != 0:
        df_probat_lr = pd.read_sql(query_probat_lr, con_probat)

    # =============================================================================
    # Section 1: Generelt
    # =============================================================================
    section_id = 1
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer', 'Varenavn', 'Basisenhed','Stregkode', 'Receptnummer',
                    'Pakkelinje', 'Produktionsdato', 'Pakketidspunkt', 'Ordrenummer',
                    'Prod.ordre status', 'Smagning status', 'Opstartssilo',
                    'Igangsat af', 'Taravægt', 'Nitrogen', 'Henstandsprøver',
                    'Referenceprøver', 'Kontrolprøver', 'Bemærkning opstart',
                    'Lotnumre produceret', 'Slat forbrug','Slat afgang',
                    'Rework forbrug', 'Rework afgang']
    columns_1_dec = ['Slat forbrug', 'Slat afgang', 'Rework forbrug', 'Rework afgang',
                     'Taravægt']
    columns_0_dec = ['Henstandsprøver','Referenceprøver','Kontrolprøver']
    columns_0_pct = ['Nitrogen']

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
            for col in columns_0_dec:
                df_nav_generelt[col] = df_nav_generelt[col].apply(lambda x: ssf.number_format(x, 'dec_0'))
            for col in columns_0_pct:
                df_nav_generelt[col] = df_nav_generelt[col].apply(lambda x: ssf.number_format(x, 'pct_0'))
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
    # Section 2: Relaterede ordrer NAV --> Probat
    # =============================================================================
    section_id = 2
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Ordrenummer','Varenummer','Navn','Relateret ordre',
                    'Relateret vare','Relateret navn','Kilde']

    if req_ordrelationstype == 0:
        df_temp_orders = pd.concat([df_nav_orders,df_probat_orders,df_nav_order_related,df_probat_lg_to_ulr])
    elif req_ordrelationstype == 1:
        df_temp_orders = pd.concat([df_nav_orders,df_probat_orders,df_probat_lg_to_ulr])
    elif req_ordrelationstype == 2:
        df_temp_orders = pd.concat([df_nav_orders,df_nav_order_related
                                    ,df_probat_orders.loc[df_probat_orders['Kilde'] == 'Probat mølle']]) # Only Probat orders which are not related to finished goods

    if ssf.get_section_status_code(df_temp_orders) == 99:
        try:
            df_temp_orders['Varenummer'] = df_temp_orders['Ordrenummer'].apply(lambda x: ssf.get_nav_order_info(x))
            df_temp_orders['Navn'] = df_temp_orders['Varenummer'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
            df_temp_orders['Relateret vare'] = df_temp_orders['Relateret ordre'].apply(lambda x: ssf.get_nav_order_info(x))
            df_temp_orders['Relateret navn'] = df_temp_orders['Relateret vare'].apply(lambda x: ssf.get_nav_item_info(x, 'Beskrivelse'))
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
                df_temp_gc_orders['Primær'] = 'Ristet kaffe' + '\n' + df_probat_lr['Ordrenummer']
                df_temp_gc_orders['Sekundær'] = 'Råkaffe' + '\n' + df_probat_lr['Kontraktnummer'] + '/' + df_probat_lr['Modtagelse']
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
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_temp_orders))

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
    # Section 4: Mølleordrer
    # =============================================================================
    section_id = 4
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Receptnummer', 'Receptnavn', 'Dato', 'Mølle',
                    'Probat id', 'Ordrenummer', 'Silo', 'Kilo']
    columns_1_dec = ['Kilo']
    columns_strip = ['Dato','Silo','Mølle']

    if ssf.get_section_status_code(df_probat_ulg) == 99:
        try:
            # Create total for dataframe
            dict_mølle_total = {'Kilo': [df_probat_ulg['Kilo'].sum()],'Probat id':None}
            # Look up column values and string format datecolumn for export
            df_probat_ulg['Receptnavn'] = df_probat_ulg['Receptnummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
            df_probat_ulg['Dato'] = df_probat_ulg['Dato'].dt.strftime('%d-%m-%Y')
            # Join multiple dates or silos to one commaseparated string
            df_probat_ulg = df_probat_ulg.groupby(['Receptnummer', 'Receptnavn',
                                                   'Probat id', 'Ordrenummer']).agg(
                                                       {'Silo': lambda x: ','.join(sorted(pd.Series.unique(x))),
                                                        'Dato': lambda x: ','.join(sorted(pd.Series.unique(x))),
                                                        'Mølle': lambda x: ','.join(sorted(pd.Series.unique(x))),
                                                        'Kilo': 'sum'
                                                       }).reset_index()
            # Remove trailing and leading commas
            for col in columns_strip:
                df_probat_ulg[col] = df_probat_ulg[col].apply(lambda x: ssf.strip_comma_from_string(x))
            # Create temp dataframe with total
            df_temp_total = pd.concat([df_probat_ulg, pd.DataFrame.from_dict(data=dict_mølle_total, orient='columns')])
            df_temp_total = df_temp_total[column_order]
            df_temp_total.sort_values(by=['Receptnummer','Ordrenummer'], inplace=True)
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
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_ulg))

    # =============================================================================
    # Section 5: Risteordrer
    # =============================================================================
    section_id = 5
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Receptnummer', 'Receptnavn', 'Dato', 'Rister',
                    'Probat id', 'Ordrenummer', 'Silo', 'Kilo']
    columns_1_dec = ['Kilo']
    columns_strip = ['Dato','Silo']

    if ssf.get_section_status_code(df_probat_ulr) == 99:
        try:
            # Create total for dataframe
            dict_rister_total = {'Kilo':[df_probat_ulr['Kilo'].sum()],'Probat id':None}
            # Look up column values and string format datecolumn for export
            df_probat_ulr['Receptnavn'] = df_probat_ulr['Receptnummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
            df_probat_ulr['Dato'] = df_probat_ulr['Dato'].dt.strftime('%d-%m-%Y')
            # Join multiple dates or silos to one commaseparated string
            df_probat_ulr = df_probat_ulr.groupby(['Receptnummer', 'Receptnavn',
                                                   'Rister','Probat id', 'Ordrenummer']).agg(
                                                       {'Silo': lambda x: ','.join(sorted(pd.Series.unique(x))),
                                                        'Dato': lambda x: ','.join(sorted(pd.Series.unique(x))),
                                                        'Kilo': 'sum'
                                                       }).reset_index()
            # Remove trailing and leading commas
            for col in columns_strip:
                df_probat_ulr[col] = df_probat_ulr[col].apply(lambda x: ssf.strip_comma_from_string(x))
            # Create temp dataframe with total
            df_temp_total = pd.concat([df_probat_ulr, pd.DataFrame.from_dict(data=dict_rister_total, orient='columns')])
            df_temp_total = df_temp_total[column_order]
            df_temp_total.sort_values(by=['Receptnummer','Ordrenummer'], inplace=True)
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
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_ulr))

    # =============================================================================
    # Section 6: Råkaffeforbrug
    # =============================================================================
    section_id = 6
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Sortnummer','Sortnavn','Kontraktnummer','Modtagelse', 'Silo',
                    'Ordrenummer','Kilo']
    columns_1_dec = ['Kilo']

    if ssf.get_section_status_code(df_probat_lr) == 99:
        try:
            # Create total for dataframe
            dict_rister_ind_total = {'Kilo':[df_probat_lr['Kilo'].sum()],'Silo':None}
             # Look up column values
            df_probat_lr['Sortnavn'] = df_probat_lr['Sortnummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
            # Create temp dataframe with total
            df_temp_total = pd.concat([df_probat_lr, pd.DataFrame.from_dict(data=dict_rister_ind_total, orient='columns')])
            df_temp_total = df_temp_total[column_order]
            df_temp_total.sort_values(by=['Ordrenummer','Sortnummer','Kilo'], inplace=True)
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
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_probat_lr))

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
    # Section 9: Rework anvendt
    # =============================================================================
    section_id = 9
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Produktionsordre','Silo',
                    'Indhold','Indhold varenummer','Indhold varenavn','Kilde']
    columns_strip = ['Kilde']

    if len(q_related_orders) != 0:
        df_rework = ssf.rework.get_rework_total(ssf.rework.get_rework_silos(q_related_orders))
    else:
        df_rework = pd.DataFrame()

    if ssf.get_section_status_code(df_rework) == 99:
        try:
            # Get additional columns through functions
            df_rework['Varenummer'] = df_rework['Produktionsordre'].apply((lambda x: ssf.get_nav_order_info(x)))
            df_rework['Varenavn'] = df_rework['Varenummer'].apply((lambda x: ssf.get_nav_item_info(x, 'Beskrivelse')))
            df_rework['Kilde varenummer'] = df_rework['Produktionsordre'].apply((lambda x: ssf.get_nav_order_info(x)))
            df_rework['Kilde varenavn'] = df_rework['Varenummer'].apply((lambda x: ssf.get_nav_item_info(x, 'Beskrivelse')))
            # Concat kilde to one string if multiple and remove any trailing or leading commas
            df_rework = df_rework.groupby(['Varenummer','Varenavn','Produktionsordre','Indhold','Indhold varenummer','Indhold varenavn','Silo']).agg(
                {'Kilde': lambda x: ','.join(sorted(pd.Series.unique(x)))}).reset_index()
            for col in columns_strip:
                df_rework[col] = df_rework[col].apply(lambda x: ssf.strip_comma_from_string(x))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_temp_total, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_rework))

    # =============================================================================
    # Section 8: Massebalance
    # =============================================================================
    section_id = 8
    section_name = ssf.get_section_name(section_id, df_sections)
    columns_1_dec = ['[1] Råkaffe','[2] Ristet kaffe','[3] Difference','[5] Færdigvaretilgang',
                     '[6] Difference','[8] Salg','[9] Regulering & ompak','[10] Restlager','[11] Difference']
    columns_2_pct = ['[4] Difference pct','[7] Difference pct','[12] Difference pct']

    dict_massebalance = {'[1] Råkaffe': df_probat_lr['Kilo'].sum(),
                         '[2] Ristet kaffe': df_probat_ulr['Kilo'].sum(),
                         '[3] Difference': None,
                         '[4] Difference pct': None,
                         '[5] Færdigvaretilgang': df_nav_færdigvaretilgang['Produceret'].sum(),
                         '[6] Difference': None,
                         '[7] Difference pct': None,
                         '[8] Salg': df_nav_færdigvaretilgang['Salg'].sum(),
                         '[9] Regulering & ompak': df_nav_færdigvaretilgang['Regulering & ompak'].sum(),
                         '[10] Restlager': df_nav_færdigvaretilgang['Restlager'].sum(),
                         '[11] Difference': None,
                         '[12] Difference pct': None}
    dict_massebalance['[3] Difference'] = dict_massebalance['[1] Råkaffe'] - dict_massebalance['[2] Ristet kaffe']
    dict_massebalance['[4] Difference pct'] = ssf.zero_division(dict_massebalance['[3] Difference'], dict_massebalance['[1] Råkaffe'], 'None')
    dict_massebalance['[6] Difference'] = dict_massebalance['[2] Ristet kaffe'] - dict_massebalance['[5] Færdigvaretilgang']
    dict_massebalance['[7] Difference pct'] = ssf.zero_division(dict_massebalance['[6] Difference'], dict_massebalance['[2] Ristet kaffe'] ,'None')
    dict_massebalance['[11] Difference'] = ( dict_massebalance['[5] Færdigvaretilgang']
        - dict_massebalance['[8] Salg'] - dict_massebalance['[9] Regulering & ompak']
        - dict_massebalance['[10] Restlager'] )
    dict_massebalance['[12] Difference pct'] = ssf.zero_division(dict_massebalance['[11] Difference'], dict_massebalance['[5] Færdigvaretilgang'], 'None')
    #Number formating
    for col in columns_1_dec:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'dec_1')
    for col in columns_2_pct:
        dict_massebalance[col] = ssf.number_format(dict_massebalance[col] ,'pct_2')

    df_massebalance = pd.DataFrame.from_dict(data=dict_massebalance, orient='index').reset_index()
    df_massebalance.columns = ['Sektion','Værdi']
    df_massebalance['Note'] = [None, None, '[1] - [2]', '[3] / [1]', None, '[2] - [5]',
                               '[6] / [2]', None, None, None, '[5] - [8] - [9] - [10]',
                               '[11] / [5]']
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
    # Section 10: Vægtkontrol
    # =============================================================================
    section_id = 10
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Registreringstidspunkt','Serienummer','Vægt','Status','Registreret af']
    columns_2_dec = ['Vægt']

    if ssf.get_section_status_code(df_ds_vægtkontrol) == 99:
        try:
            df_ds_vægtkontrol = df_ds_vægtkontrol[column_order]
            df_ds_vægtkontrol['Registreringstidspunkt'] = df_ds_vægtkontrol['Registreringstidspunkt'].dt.strftime('%d-%m-%Y %H:%M')
            #Column formating
            for col in columns_2_dec:
                df_ds_vægtkontrol[col] = df_ds_vægtkontrol[col].apply(lambda x: ssf.number_format(x, 'dec_2'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_ds_vægtkontrol, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_ds_vægtkontrol))

    # =============================================================================
    # Section 11: Ordrestatistik fra e-vejning (poser)
    # =============================================================================
    section_id = 11
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Total vægt kg', 'Antal poser', 'Middelvægt g', 'Standardafvigelse g',
                    'Gns. godvægt per enhed g', 'Godvægt total g', 'Nominel vægt g', 'Taravægt g']
    columns_2_dec = ['Total vægt kg', 'Antal poser', 'Middelvægt g', 'Standardafvigelse g',
                    'Gns. godvægt per enhed g', 'Godvægt total g', 'Nominel vægt g', 'Taravægt g']

    if ssf.get_section_status_code(df_com_statistics) == 99:
        try:
            df_com_statistics = df_com_statistics[column_order]
            #Column formating
            for col in columns_2_dec:
                df_com_statistics[col] = df_com_statistics[col].apply(lambda x: ssf.number_format(x, 'dec_2'))
            # Transpose dataframe
            df_com_statistics = df_com_statistics.transpose()
            df_com_statistics = df_com_statistics.reset_index()
            df_com_statistics.columns = ['Sektion','Værdi']
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_com_statistics, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_com_statistics))

    # =============================================================================
    # Section 12: Karakterer
    # =============================================================================
    section_id = 12
    section_name = ssf.get_section_name(section_id, df_sections)
    columns_1_dec = ['Syre','Krop','Aroma','Eftersmag','Robusta']

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
    # Section 13: Komponentforbrug
    # =============================================================================
    section_id = 13
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Basisenhed','Antal']
    columns_1_dec = ['Antal']

    if ssf.get_section_status_code(df_nav_consumption) == 99:
        try:
            df_nav_consumption = df_nav_consumption[column_order]
            # Data formating
            for col in columns_1_dec:
                df_nav_consumption[col] = df_nav_consumption[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_nav_consumption, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_nav_consumption))

    # =============================================================================
    # Section 14: Anvendt primæremballage
    # =============================================================================
    section_id = 14
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Varenummer','Varenavn','Lotnummer','Rullenummer','Rullelængde',
                    'Pakkedato','Købsordre']

    if ssf.get_section_status_code(df_nav_components) == 99:
        try:
            df_nav_components = pd.concat([df_nav_components, df_ds_ventil])
            df_nav_components['Varenavn'] = df_nav_components['Varenummer'].apply(ssf.get_nav_item_info, field='Beskrivelse')
            df_nav_components = df_nav_components[column_order]
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_nav_components, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_nav_components))

    # =============================================================================
    # Section 15: Lotnumre
    # =============================================================================
    section_id = 15
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Lotnummer', 'Pallenummer', 'Produktionstidspunkt', 'Kontrolleret af',
                    'Kontrol bemærkning', 'Kontroltidspunkt', 'Kilo', 'Antal poser',
                    'Antal leakers', 'Leakers pct', 'Resultat af kontrol']
    columns_0_dec = ['Antal poser','Antal leakers']
    columns_1_dec = ['Kilo']
    columns_2_pct = ['Leakers pct']

    if ssf.get_section_status_code(df_nav_lotno) == 99:
        try:
            df_nav_lotno = pd.merge(df_nav_lotno, df_ds_vacslip, left_on = 'Lotnummer',
                                    right_on = 'Lotnummer', how='left', suffixes=('', '_y'))
            df_nav_lotno['Antal leakers'].fillna(value=0, inplace=True)
            df_nav_lotno['Resultat af kontrol'].fillna(value='Ej kontrolleret', inplace=True)
            df_nav_lotno['Leakers pct'] = df_nav_lotno.apply(lambda x: ssf.zero_division(x['Antal leakers'], x['Antal poser'], 'Zero'), axis=1)
            df_nav_lotno['Pallenummer'] = df_nav_lotno['Pallenummer_y'].fillna(df_nav_lotno['Pallenummer'])
            df_nav_lotno['Produktionstidspunkt'] = df_nav_lotno['Produktionstidspunkt'].dt.strftime('%d-%m-%Y %H:%M')
            df_nav_lotno = df_nav_lotno[column_order]
            # Data formating
            for col in columns_1_dec:
                df_nav_lotno[col] = df_nav_lotno[col].apply(lambda x: ssf.number_format(x, 'dec_1'))
            # Data formating
            for col in columns_0_dec:
                df_nav_lotno[col] = df_nav_lotno[col].apply(lambda x: ssf.number_format(x, 'dec_0'))
            # Data formating
            for col in columns_2_pct:
                df_nav_lotno[col] = df_nav_lotno[col].apply(lambda x: ssf.number_format(x, 'pct_2'))
            # Write results to Excel
            ssf.insert_dataframe_into_excel(excel_writer, df_nav_lotno, section_name, False)
            # Write status into log
            ssf.section_log_insert(req_id, section_id, 0)
        except Exception as e: # Insert error into log
            ssf.section_log_insert(req_id, section_id, 2, e)
    else: # Write into log if no data is found or section is out of scope
        ssf.section_log_insert(req_id, section_id, ssf.get_section_status_code(df_nav_lotno))

    # =============================================================================
    # Section 16: Reference- og henstandsprøver
    # =============================================================================
    section_id = 16
    section_name = ssf.get_section_name(section_id, df_sections)
    column_order = ['Id', 'Registreringstidspunkt', 'Operatør', 'Silo', 'Prøvetype',
                    'Bemærkning', 'Smagning status', 'Antal prøver']
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
