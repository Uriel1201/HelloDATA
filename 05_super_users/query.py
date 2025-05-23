# python -m pip install oracledb numpy pyarrow polars --upgrade
# import numpy as np
# import polars as pl
# import oracledb
# import pyarrow

try:
    # You need to write your credentials in the following connection.
    conn = oracledb.connect(user = "[Username]", password = "[Password]", dsn = "localhost:1521/FREEPDB1")
    table = "SELECT * FROM USERS_P5"
    odf = conn.fetch_df_all(statement = table, arraysize=100)
    pyarrow_table = pyarrow.Table.from_arrays(odf.column_arrays(), names = odf.column_names())
    users = pl.from_arrow(pyarrow_table).lazy()

    '''
    Alternative 2: Querying directly from this repository 
    url = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/05_super_users/data.tsv"
    users = pl.scan_csv(url,
                        separator = "\t",
                        has_header = True,
                        infer_schema_length = 1000,
                        ignore_errors = False
               )
    '''
    
    sample = users.head(5)
    print(f'\nUSERS TABLE (SAMPLE): \n{sample.collect()}')

    df = (sample.sort(by=['USER_ID','TRANSACTION_DATE'])
                .with_columns(SUPERUSER_DATE = pl.col('TRANSACTION_DATE')
                                                 .shift(-1)
                                                 .over(partition_by = 'USER_ID')
                 )
         )
    print(f'\nIDENTIFYING DATES AS SUPER (SAMPLE):\n{df.collect()}')
    superusers = (users.sort(by=['USER_ID','TRANSACTION_DATE'])
                       .with_columns(SUPERUSER_DATE = pl.col('TRANSACTION_DATE')
                                                        .shift(-1)
                                                        .over(partition_by = 'USER_ID')
                        )
                       .group_by('USER_ID')
                       .first()
                       .select(pl.col('USER_ID')
                               ,pl.col('SUPERUSER_DATE')
                        )
                 )
    print(f'\nRETURNING DATE WHEN USER GOT SUPERUSER MEMBERSHIP:\n{superusers.collect()}')          
finally:
    conn.close()
