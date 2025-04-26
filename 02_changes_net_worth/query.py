# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM TRANSACTIONS_P2"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    transactions=pl.from_arrow(pyarrow_table)
    changes=(transactions.unpivot(on=['SENDER','RECEIVER']
                                  ,index='AMOUNT' 
                                  ,variable_name='TYPE' 
                                  ,value_name='USER_ID'
                          )
                         .with_columns(pl.when(pl.col('TYPE')=='SENDER')
                                         .then(pl.col('AMOUNT')*-1)
                                         .otherwise(pl.col('AMOUNT'))
                                         .alias('AMOUNT')
                          )
                         .group_by('USER_ID')
                         .agg(pl.col('AMOUNT')
                                .sum()
                          )
                         .sort(by='AMOUNT' 
                               ,descending=True
                          )
    )
    print(f'Net changes using Polars:\n{changes}')                 

finally:
    conn.close()
