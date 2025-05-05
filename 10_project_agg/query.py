# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM FRIENDS_P9"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    friends=pl.from_arrow(pyarrow_table).lazy()
    friendship=(pl.sql("""
                       SELECT USER_1 as USER_ID FROM friends
                       UNION ALL
                       SELECT USER_2 as USER_ID FROM friends
                       """
                   )
                  .group_by('*')
                  .agg(pl.len()
                         .alias('NUMBER_OF_FRIENDS')
                   )
                  .sort(by='NUMBER_OF_FRIENDS',
                        descending=True
                   )
    ).collect()
    print(f'Returning number of friends by each user:\n{friendship}')
              
finally:
    conn.close()
