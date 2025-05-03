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
                       SELECT user_1 as user_id FROM pl_friends
                       UNION ALL
                       SELECT user_2 as user_id FROM pl_friends
                       """
                   )
                  .group_by('*')
                  .agg(pl.len()
                         .alias('number_of_friends')
                   )
                  .sort(by='number_of_friends',
                        descending=True
                   )
    ).collect()
    print(f'Returning number of friends by each user:\n{friendship}')
              
finally:
    conn.close()
