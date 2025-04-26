# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM ITEMS_P3"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    items=pl.from_arrow(pyarrow_table)
    polars_counts=(items.group_by(['DATES','ITEM'])
                           .agg(pl.len()
                                  .alias('COUNT')
                            )
                           .with_columns(pl.max('COUNT')
                                           .over(partition_by='DATES')
                                           .alias('MAX_COUNT')
                            )
                           .filter(pl.col('COUNT')==pl.col('MAX_COUNT')
                            )
                           .select(pl.col('DATES'),
                                   pl.col('ITEM')
                            )
    )
    print(f'item most frequented for each date using Polars:\n{polars_counts}')          
finally:
    conn.close()
