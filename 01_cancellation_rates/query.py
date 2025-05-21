# python -m pip install oracledb numpy pyarrow polars --upgrade
# import numpy as np
# import polars as pl
# import oracledb
# import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM USERS_P1"
    odf=conn.fetch_df_all(statement=table,arraysize=100)
    pyarrow_table=pyarrow.Table.from_arrays(odf.column_arrays(),names=odf.column_names())
    users=pl.from_arrow(pyarrow_table)
    
    '''
    Alternative 2: Querying directly from this repository 
    url="https://raw.githubusercontent.com/Uriel1201/HelloSQL2.0/refs/heads/main/01_cancellation_rates/data.tsv"
    users=pl.scan_csv(url,
                      separator="\t",
                      has_header=True,
                      infer_schema_length=1000,
                      ignore_errors=False
    )
    users=users.collect()
    '''
    
    rates=(users.to_dummies(columns='ACTION')
                .drop('DATES')
                .group_by('USER_ID')
                .agg(pl.col('*').sum())
                .select(pl.col('USER_ID'),
                        publish_rate=pl.col('ACTION_publish')/pl.col('ACTION_start'),
                        cancel_rate=pl.col('ACTION_cancel')/pl.col('ACTION_start')
                 )
    )
    print(f'Rates for each user using Polars:\n{rates}')                 

finally:
    conn.close()
