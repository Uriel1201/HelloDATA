# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM USERS_P1"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    users=pl.from_arrow(pyarrow_table)
    rates=(users.to_dummies(columns='action')
                .drop('dates')
                .group_by('user_id')
                .agg(pl.col('*').sum())
                .select(pl.col('user_id'),
                        pl.col('action_publish')/pl.col('action_start'),
                                  pl.col('action_cancel')/pl.col('action_start')
                           )
)
print(f'Rates for each user using Polars:')
polars_rates
                          
finally:
    conn.close()
