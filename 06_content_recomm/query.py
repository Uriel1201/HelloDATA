# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    # You need to write your credentials in the following connection.
    conn = oracledb.connect(user = "[Username]", password = "[Password]", dsn = "localhost:1521/FREEPDB1")
    table1 = "SELECT * FROM FRIENDS_P6"
    table2 = "SELECT * FROM LIKES_P6"
    odf1 = conn.fetch_df_all(statement = table1, arraysize = 100)
    odf2 = conn.fetch_df_all(statement = table2, arraysize = 100)
    pyarrow_table1 = pyarrow.Table.from_arrays(odf1.column_arrays(), names = odf1.column_names())
    pyarrow_table2 = pyarrow.Table.from_arrays(odf2.column_arrays(), names = odf2.column_names())
    friends = pl.from_arrow(pyarrow_table1).lazy()
    likes = pl.from_arrow(pyarrow_table2).lazy()

    '''
    Alternative 2: Querying directly from this repository 
    url_1 = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/06_content_recomm/data_friends.tsv"
    url_2 = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/06_content_recomm/data_likes.tsv"
    friends = pl.scan_csv(url_1,
                          separator = "\t",
                          has_header = True,
                          infer_schema_length = 1000,
                          ignore_errors = False
                 )
    likes = pl.scan_csv(url_2,
                        separator = "\t",
                        has_header = True,
                        infer_schema_length = 1000,
                        ignore_errors = False
               )
    '''
    
    lf = (friends.join(likes.select(pl.col('USER_ID')
                                      .alias('FRIEND'),
                                    pl.col('PAGE_LIKES')
                             )
                       ,on = 'FRIEND'
                       ,how = 'right'
                  )
                 .select(pl.col('USER_ID'),
                         pl.col('PAGE_LIKES')
                  )
         )
    recommendations = (lf.join(likes,
                               on = ['USER_ID','PAGE_LIKES'],
                               how = 'anti'
                          )
                         .unique()
                         .select(pl.col('USER_ID'),
                                 pl.col('PAGE_LIKES')
                                   .alias('RECOMMENDATION')
                          )
                         .sort(by = 'USER_ID')
                      )
    print(f'\nRecommendations:\n{recommendations.collect()}')          
finally:
    conn.close()
