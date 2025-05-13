# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM EMPLOYEE_P14"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    employee=pl.from_arrow(pyarrow_table).lazy()
    cumulative=(employee.sort(by=['ID','PAY_MONTH'])
                        .with_columns(CUMULATIVE_PAY=pl.col('SALARY')
                                                       .cum_sum()
                                                       .over('ID'),
                                      RANK=pl.col('PAY_MONTH')
                                             .rank(method='dense',
                                                   descending=True
                                              )
                                             .over('ID')
                         )
                        .filter(pl.col('RANK')>1)
                        .select(pl.col('ID'),
                                pl.col('PAY_MONTH'),
                                pl.col('CUMULATIVE_PAY')
                         )
    )
    print(f'Returning the cumulative pays excluding the last month:\n {cumulative.collect()}')
     
finally:
    conn.close()
