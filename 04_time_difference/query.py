# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM USERS_P4"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    users=pl.from_arrow(pyarrow_table)
    durations=(polars_users.sort(by=['ID','ACTION_DATE']
                                     ,descending=[False,True]
                            )
                           .with_columns(ELAPSED_TIME=pl.col('ACTION_DATE')
                                                        .diff(-1)
                                                        .dt
                                                        .total_days()
                                                        .over(partition_by='ID')
                            )
                           .group_by('ID')
                           .first()
                           .select(pl.col('ID'),
                                   pl.col('ELAPSED_TIME')
                            )
    )
    print(f'Returning time elapsed using Polars:\n{durations}')          
finally:
    conn.close()
