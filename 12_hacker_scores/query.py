# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table1="SELECT * FROM HACKERS_P12"
    table2="SELECT * FROM SUBMISSIONS_P12"
    odf1=conn.fetch_df_all(statement=table1,arraysize=100)
    odf2=conn.fetch_df_all(statement=table2,arraysize=100)
    pyarrow_table1=pyarrow.Table.from_arrays(odf1.column_arrays(),names=odf1.column_names())
    pyarrow_table2=pyarrow.Table.from_arrays(odf2.column_arrays(),names=odf2.column_names())
    hackers=pl.from_arrow(pyarrow_table1).lazy()
    submissions=pl.from_arrow(pyarrow_table2).lazy()
    scores=(submissions.select(pl.col('HACKER_ID'),
                               pl.col('CHALLENGE_ID'),
                               pl.col('SCORE')
                        )
                       .group_by(['HACKER_ID','CHALLENGE_ID'])
                       .agg(MAX_SCORE=pl.max('SCORE')
                        )
                       .group_by('HACKER_ID')
                       .agg(TOTAL_SCORE=pl.sum('MAX_SCORE')
                        )
                       .join(hackers,
                             on='HACKER_ID',
                             how='right'
                        )
                       .filter(pl.col('TOTAL_SCORE')>0)
                       .sort(by=['TOTAL_SCORE','HACKER_ID'],
                             descending=[True,False]
                        )
    ).collect()
    print(f"Querying users who don't have score equal to zero:\n{scores.head(5)}")

finally:
    conn.close()
