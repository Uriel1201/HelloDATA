# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table1="SELECT * FROM USERS_P8"
    table2="SELECT * FROM EVENTS_P8"
    odf1=conn.fetch_df_all(statement=table1,arraysize=100)
    odf2=conn.fetch_df_all(statement=table2,arraysize=100)
    pyarrow_table1=pyarrow.Table.from_arrays(odf1.column_arrays(),names=odf1.column_names())
    pyarrow_table2=pyarrow.Table.from_arrays(odf2.column_arrays(),names=odf2.column_names())
    users=pl.from_arrow(pyarrow_table1).lazy()
    events=pl.from_arrow(pyarrow_table2).lazy()          
finally:
    conn.close()
