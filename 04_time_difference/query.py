# python -m pip install oracledb numpy pyarrow polars --upgrade
# import numpy as np
# import polars as pl
# import oracledb
# import pyarrow

try:
    # You need to write your credentials in the following connection.
    conn = oracledb.connect(user = "[Username]", password = "[Password]", dsn = "localhost:1521/FREEPDB1")
    table = "SELECT * FROM USERS_P4"
    odf = conn.fetch_df_all(statement = table, arraysize = 100)
    pyarrow_table = pyarrow.Table.from_arrays(odf.column_arrays(), names = odf.column_names())
    users = pl.from_arrow(pyarrow_table).lazy()

    '''
    Alternative 2: Querying directly from this repository 
    url = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/04_time_difference/data.tsv"
    users = pl.scan_csv(url,
                        separator = "\t",
                        has_header = True,
                        infer_schema_length = 1000,
                        ignore_errors = False
               )
    '''
    
    sample = users.head(5)
    print(f'USERS TABLE (SAMPLE -> 5):\n{sample.collect()}')

    df = (sample.sort(by = ['ID','ACTION_DATE']
                      ,descending=[False,True]
                 )
                .with_columns(ELAPSED_TIME = pl.col('ACTION_DATE')
                                               .diff(-1)
                                               .dt
                                               .total_days()
                                               .over(partition_by = 'ID')
                 )
         )
    print(f'\nTIME DIFFERENCE BETWEEN CONSECUTIVE ACTIONS (SAMPLE -> 5):\n{df.collect()}')
    
    durations = (users.sort(by = ['ID','ACTION_DATE']
                            ,descending=[False,True]
                       )
                      .with_columns(ELAPSED_TIME = pl.col('ACTION_DATE')
                                                     .diff(-1)
                                                     .dt
                                                     .total_days()
                                                     .over(partition_by = 'ID')
                       )
                      .group_by('ID')
                      .first()
                      .select(pl.col('ID'),
                              pl.col('ELAPSED_TIME')
                       )
                )
    print(f'\nRETURNING ELAPSED TIME BETWEEN THE TWO LAST ACTIVITIES:\n{durations.collect()}')          
finally:
    conn.close()
