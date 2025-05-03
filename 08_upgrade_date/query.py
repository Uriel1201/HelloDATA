# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table1="SELECT * FROM MOBILE_P7"
    table2="SELECT * FROM WEB_P7"
    odf1=conn.fetch_df_all(statement=table1,arraysize=100)
    odf2=conn.fetch_df_all(statement=table2,arraysize=100)
    pyarrow_table1=pyarrow.Table.from_arrays(odf1.column_arrays(),names=odf1.column_names())
    pyarrow_table2=pyarrow.Table.from_arrays(odf2.column_arrays(),names=odf2.column_names())
    mobile=pl.from_arrow(pyarrow_table1).lazy()
    web=pl.from_arrow(pyarrow_table2).lazy()
    fractions=(mobile.select(pl.col('USER_ID'),
                             pl.col('USER_ID')
                               .alias('WEB_USER')
                      )
                     .unique()
                     .join(web.select(pl.col('USER_ID'),
                                      pl.col('USER_ID')
                                        .alias('MOBILE_USER')

                               )
                              .unique(),
                           on='USER_ID',
                           how='full',
                           coalesce=True
                      )
                     .with_columns(MOBILE_USER=pl.when(pl.col('MOBILE_USER')
                                                         .is_null()
                                                  )
                                                 .then(1)
                                                 .otherwise(0),
                                   WEB_USER=pl.when(pl.col('WEB_USER')
                                                      .is_null()
                                               )
                                              .then(1)
                                              .otherwise(0),
                                   BOTH=pl.when(pl.col('MOBILE_USER')==pl.col('WEB_USER'))
                                          .then(1)
                                          .otherwise(0)
                       )
                      .select(pl.mean('WEB_USER',
                                      'MOBILE_USER',
                                      'BOTH'
                                 )
                       )
    ).collect()
    print(f'Fractions:{fractions}')          
finally:
    conn.close()
