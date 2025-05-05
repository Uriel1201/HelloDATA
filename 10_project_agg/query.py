# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM PROJECTS_P10"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    projects=pl.from_arrow(pyarrow_table).lazy()
    durations=(projects.sort(by='START_DATE')
                       .with_columns(PROJECT_ID=pl.when(pl.col('START_DATE').eq(pl.col('END_DATE')
                                                                                  .shift(1)
                                                                             )
                                                   )
                                                  .then(0)
                                                  .otherwise(1)
                                                  .cum_sum()
                        )
                       .group_by('PROJECT_ID')
                       .agg(PROJECT_START=pl.first('START_DATE'),
                            PROJECT_END=pl.last('END_DATE')
                        )
                       .with_columns(DURATION=(pl.col('PROJECT_END')-pl.col('PROJECT_START')).dt
                                                                                             .total_days()
                        )
                       .sort(by='DURATION')
    ).collect()
    print(f'Duration of each unique project:\n{durations}')

finally:
    conn.close()
