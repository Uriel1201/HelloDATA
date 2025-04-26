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
    polars_counts=(polars_items.group_by(['dates','item'])
                           .agg(pl.len()
                                  .alias('count')
                            )
                           .with_columns(pl.max('count')
                                           .over(partition_by='dates')
                                           .alias('max_count')
                            )
                           .filter(pl.col('count')==pl.col('max_count')
                            )
                           .select(pl.col('dates'),
                                   pl.col('item')
                            )
)
print(f'item most frequented for each date using Polars:')
polars_counts             
finally:
    conn.close()
