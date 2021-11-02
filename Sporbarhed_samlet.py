#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import pandas as pd
import pyodbc
import Sporbarhed_råkaffe
import Sporbarhed_færdigkaffe


# =============================================================================
# Variables for query connections
# =============================================================================
server_04 = 'sqlsrv04'
db_04 = 'BKI_Datastore'
con_04 = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_04};DATABASE={db_04};autocommit=True')

# =============================================================================
# Read data from request
# =============================================================================
query_ds_request =  """ SELECT TOP 1 [Id] ,[Forespørgselstype],[Rapport_modtager]
                    ,[Referencenummer] ,[Note_forespørgsel] ,[Modtagelse]  ,[Ordrerelationstype]
                    FROM [trc].[Sporbarhed_forespørgsel]
                    WHERE [Forespørgsel_igangsat] IS NULL """
df_request = pd.read_sql(query_ds_request, con_04)

# Exit script if no request data is found
if len(df_request) == 0:
    raise SystemExit(0)

# =============================================================================
# Set request variables
# =============================================================================
req_type = df_request.loc[0, 'Forespørgselstype']
req_id = df_request.loc[0, 'Id']

# =============================================================================
# Execute correct script
# =============================================================================

if req_type == 0:
    Sporbarhed_færdigkaffe.initiate_report(req_id)
elif req_type == 1:
    Sporbarhed_råkaffe.initiate_report(req_id)
elif req_type == 2:
    pass


# Exit script
raise SystemExit(0)
