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

    space = "/*****************************************/"
    sample = items.head(5)
    print(f'\n{space}')
    print(f'ITEMS TABLE -> SAMPLE:\n{sample.collect()}')

    df = (sample.group_by(['DATES','ITEM'])
                .agg(pl.len()
                       .alias('COUNT')
                 )
         )
    print(f'\n{space}')
    print(f'NUMBER OF ITEMS BY EACH DATE -> SAMPLE:\n{df.collect()}')

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
    print(f'\n{space}')
    print(f'MOST FREQUENTED ITEM BY DATE:\n{counts.collect()}')
finally:
    conn.close()
