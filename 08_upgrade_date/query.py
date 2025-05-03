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
    lf=(users.select(pl.col('USER_ID'),
                     pl.col('JOIN_DATE')
              )
             .join(events.filter(pl.col('TYPE')=='F2')
                         .select(pl.col('USER_ID')),
                   on='USER_ID',
                   how='inner'
              )
             .join(events.filter(pl.col('TYPE')=='P')
                         .select(pl.col('USER_ID'),
                                 pl.col('ACCESS_DATE')
                          ),
                   on='USER_ID',
                   how='left'
              )
             .with_columns(DURATION=(pl.col('ACCESS_DATE')-pl.col('JOIN_DATE')).dt.total_days()
              )
             .select(pl.col('USER_ID'),
                     pl.col('DURATION')
              )
    ).collect()
    print(f'Elapsed time between\njoin date and access as premium:\n{lf}')
    ratio=(lf.select((pl.col('DURATION')<=30.0).fill_null(False)
                                               .mean()
                                               .round(2)
              )
    )
    print(f'\nfraction of users that updated\nwithin the first 30 days:\n{ratio}')
finally:
    conn.close()
