# python -m pip install oracledb numpy pyarrow polars --upgrade
# import numpy as np
# import polars as pl
# import oracledb
# import pyarrow

try:
    # You need to write your credentials in the following connection.
    conn = oracledb.connect(user = "[Username]", password = "[Password]", dsn = "localhost:1521/FREEPDB1")
    table = "SELECT * FROM ITEMS_P3"
    odf = conn.fetch_df_all(statement = table, arraysize = 100)
    pyarrow_table = pyarrow.Table.from_arrays(odf.column_arrays(), names = odf.column_names())
    items = pl.from_arrow(pyarrow_table).lazy()

    '''
    Alternative 2: Querying directly from this repository 
    url = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/03_frequent_item/data.tsv"
    items = pl.scan_csv(url,
                        separator = "\t",
                        has_header = True,
                        infer_schema_length = 1000,
                        ignore_errors = False
               )
    '''

    sample = items.head(5)
    print(f'Items table SAMPLE(5):\n{sample.collect()}')

    df = (sample.group_by(['DATES','ITEM'])
                .agg(pl.len()
                       .alias('COUNT')
                 )
         )
    print(f'\nNumber of items by each date (SAMPLE):\n{df.collect()}')
    
    counts = (items.group_by(['DATES','ITEM'])
                   .agg(pl.len()
                          .alias('COUNT')
                    )
                   .with_columns(pl.max('COUNT')
                                   .over(partition_by = 'DATES')
                                   .alias('MAX_COUNT')
                    )
                   .filter(pl.col('COUNT') == pl.col('MAX_COUNT')
                    )
                   .select(pl.col('DATES'),
                           pl.col('ITEM')
                    )
                   .sort(by = 'DATES')
             )
    print(f'\nMost frequented item by each date:\n{counts.collect()}')          
finally:
    conn.close()
