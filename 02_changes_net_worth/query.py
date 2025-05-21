# python -m pip install oracledb numpy pyarrow polars --upgrade
# import numpy as np
# import polars as pl
# import oracledb
# import pyarrow

try:
    # You need to write your credentials in the following connection.
    conn = oracledb.connect(user = "[Username]", password = "[Password]", dsn = "localhost:1521/FREEPDB1")
    table = "SELECT * FROM TRANSACTIONS_P2"
    odf = conn.fetch_df_all(statement = table, arraysize = 100)
    pyarrow_table = pyarrow.Table.from_arrays(odf.column_arrays(), names = odf.column_names())
    transactions = pl.from_arrow(pyarrow_table).lazy()

    '''
    Alternative 2: Querying directly from this repository 
    url = "https://raw.githubusercontent.com/Uriel1201/HelloSQL2.0/refs/heads/main/02_changes_net_worth/data.tsv"
    transactions = pl.scan_csv(url,
                               separator = "\t",
                               has_header = True,
                               infer_schema_length = 1000,
                               ignore_errors = False
                      )
    '''
    sample = transactions.head(5)
    print(f'Transactions table SAMPLE(5):\n{sample.collect()}')

    _type = (sample.unpivot(on = ['SENDER', 'RECEIVER'],
                            index = 'AMOUNT',
                            variable_name = 'TYPE',
                            value_name = 'USER_ID'
                    )
                   .with_columns(pl.when(pl.col('TYPE') == 'SENDER')
                                           .then(pl.col('AMOUNT') * -1)
                                           .otherwise(pl.col('AMOUNT'))
                                           .alias('AMOUNT')
                                )
            )
    print(f'\nType of transaction made by each user:\n{_type.collect()}')
    
    changes = (transactions.unpivot(on = ['SENDER','RECEIVER']
                                    ,index = 'AMOUNT' 
                                    ,variable_name = 'TYPE' 
                                    ,value_name = 'USER_ID'
                            )
                           .with_columns(pl.when(pl.col('TYPE') == 'SENDER')
                                           .then(pl.col('AMOUNT') * -1)
                                           .otherwise(pl.col('AMOUNT'))
                                           .alias('AMOUNT')
                            )
                           .group_by('USER_ID')
                           .agg(pl.col('AMOUNT')
                                  .sum()
                            )
                           .sort(by = 'AMOUNT' 
                                 ,descending = True
                            )
              )
    print(f'\nNet changes:\n{changes.collect()}')                 

finally:
    conn.close()
