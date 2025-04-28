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
    durations=(polars_users.sort(by=['User_id','Action_date']
                                     ,descending=[False,True]
                            )
                           .with_columns(Elapsed_time=pl.col('Action_date')
                                                        .diff(-1)
                                                        .dt
                                                        .total_days()
                                                        .over(partition_by='User_id')
                            )
                           .group_by('User_id')
                           .first()
                           .select(pl.col('User_id'),
                                   pl.col('Elapsed_time')
                            )
    )
    print(f'Returning time elapsed using Polars:\n{durations}')          
finally:
    conn.close()
