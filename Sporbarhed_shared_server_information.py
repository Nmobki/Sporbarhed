#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import urllib
from sqlalchemy import create_engine
import pyodbc


# =============================================================================
# Server connections
# =============================================================================

server_ds = 'sqlsrv04'
db_ds = 'BKI_Datastore'
con_ds = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_ds};DATABASE={db_ds};autocommit=True')
params_ds = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_ds};DATABASE={db_ds};Trusted_Connection=yes')
engine_ds = create_engine(f'mssql+pyodbc:///?odbc_connect={params_ds}')
cursor_ds = con_ds.cursor()

server_nav = r'SQLSRV03\NAVISION'
db_nav = 'NAV100-DRIFT'
con_nav = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_nav};DATABASE={db_nav};Trusted_Connection=yes')
params_nav = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_nav};DATABASE={db_nav};Trusted_Connection=yes')
engine_nav = create_engine(f'mssql+pyodbc:///?odbc_connect={params_nav}')

server_comscale = r'comscale-bki\sqlexpress'
db_comscale = 'ComScaleDB'
con_comscale = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_comscale};DATABASE={db_comscale}')
params_comscale = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_comscale};DATABASE={db_comscale};Trusted_Connection=yes')
engine_comscale = create_engine(f'mssql+pyodbc:///?odbc_connect={params_comscale}')

server_probat = '192.168.125.161'
db_probat = 'BKI_IMP_EXP'
con_probat = pyodbc.connect(f'DRIVER=SQL Server;SERVER={server_probat};DATABASE={db_probat};uid=bki_read;pwd=Probat2016')
params_probat = urllib.parse.quote_plus(f'DRIVER=SQL Server Native Client 11.0;SERVER={server_probat};DATABASE={db_probat};Trusted_Connection=yes')
engine_probat = create_engine(f'mssql+pyodbc:///?odbc_connect={params_probat}')


# =============================================================================
# Filepaths
# =============================================================================

report_filepath = r'\\filsrv01\BKI\11. Økonomi\04 - Controlling\NMO\4. Kvalitet\Sporbarhedstest\Tests via PowerApps'