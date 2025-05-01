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
    fractions=(mobile.select(pl.col('user_id'),
                             pl.col('user_id')
                               .alias('web_user')
                      )
                     .unique()
                     .join(web.select(pl.col('user_id'),
                                      pl.col('user_id')
                                        .alias('mobile_user')

                               )
                              .unique(),
                           on='user_id',
                           how='full',
                           coalesce=True
                      )
                     .with_columns(mobile_user=pl.when(pl.col('mobile_user')
                                                         .is_null()
                                                  )
                                                 .then(1)
                                                 .otherwise(0),
                                   web_user=pl.when(pl.col('web_user')
                                                      .is_null()
                                                 )
                                                .then(1)
                                                .otherwise(0),
                                     both=pl.when(pl.col('mobile_user')==pl.col('web_user'))
                                            .then(1)
                                            .otherwise(0)
                       )
                      .select(pl.mean('web_user',
                                      'mobile_user',
                                      'both'
                                 )
                       )
).collect()
    print(f'Fractions:{fractions}')          
finally:
    conn.close()
