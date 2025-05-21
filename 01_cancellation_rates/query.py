# python -m pip install oracledb numpy pyarrow polars --upgrade
# import numpy as np
# import polars as pl
# import oracledb
# import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM USERS_P1"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    users=pl.from_arrow(pyarrow_table)
    rates=(users.to_dummies(columns='ACTION')
                .drop('DATES')
                .group_by('USER_ID')
                .agg(pl.col('*').sum())
                .select(pl.col('USER_ID'),
                        publish_rate=pl.col('ACTION_publish')/pl.col('ACTION_start'),
                        cancel_rate=pl.col('ACTION_cancel')/pl.col('ACTION_start')
                 )
    )
    print(f'Rates for each user using Polars:{rates}')                 

finally:
    conn.close()
