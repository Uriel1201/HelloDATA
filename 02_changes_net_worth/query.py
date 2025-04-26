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
    changes=(transactions.unpivot(on=['Sender','Receiver']
                                  ,index='Amount'
                                  ,variable_name='Type'
                                  ,value_name='User_id'
                          )
                         .with_columns(pl.when(pl.col('Type')=='Sender')
                                         .then(pl.col('Amount')*-1)
                                         .otherwise(pl.col('Amount'))
                                         .alias('Amount')
                          )
                         .group_by('User_id')
                         .agg(pl.col('Amount')
                                .sum()
                          )
                         .sort(by='Amount'
                               ,descending=True
                          )
    )
    print(f'Net changes using Polars:\n{changes}')                 

finally:
    conn.close()
