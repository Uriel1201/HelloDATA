# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table1="SELECT * FROM TEAMS_P15"
    table2="SELECT * FROM MATCHES_P15"
    odf1=conn.fetch_df_all(statement=table1,arraysize=100)
    odf2=conn.fetch_df_all(statement=table2,arraysize=100)
    pyarrow_table1=pyarrow.Table.from_arrays(odf1.column_arrays(),names=odf1.column_names())
    pyarrow_table2=pyarrow.Table.from_arrays(odf2.column_arrays(),names=odf2.column_names())
    teams=pl.from_arrow(pyarrow_table1).lazy()
    matches=pl.from_arrow(pyarrow_table2).lazy()
    scores=(matches.select(pl.col('HOST_TEAM')
                             .alias('TEAM_ID'),
                           pl.col('HOST_GOALS')
                             .alias('H'),
                           pl.col('GUEST_GOALS')
                             .alias('G')
                    )
                   .with_columns(SCORE_H=pl.when(pl.col('H')>pl.col('G'))
                                           .then(3)
                                           .otherwise(pl.when(pl.col('H')<pl.col('G'))
                                                        .then(0)
                                                        .otherwise(1)
                                            )
                    )
                   .select(pl.col('TEAM_ID'),
                           pl.col('SCORE_H')
                    )
                   .join(matches.select(pl.col('GUEST_TEAM')
                                          .alias('TEAM_ID'),
                                        pl.col('HOST_GOALS')
                                          .alias('H'),
                                        pl.col('GUEST_GOALS')
                                          .alias('G')
                                 )
                                .with_columns(SCORE_G=pl.when(pl.col('H')>pl.col('G'))
                                                        .then(0)
                                                        .otherwise(pl.when(pl.col('H')<pl.col('G'))
                                                                     .then(3)
                                                                     .otherwise(1)
                                                         )
                                 )
                                .select(pl.col('TEAM_ID'),
                                        pl.col('SCORE_G')
                                 ),
                          on='TEAM_ID',
                          how='full',
                          coalesce=True
                    )
    )
    teams_rank=(teams.join(scores,
                           on='TEAM_ID',
                           how='left'
                      )
                     .with_columns(SCORE=pl.col('SCORE_H')+pl.col('SCORE_G')
                      )
                     .select(pl.col('TEAM_NAME'),
                             pl.col('SCORE')
                      )
                     .sort(by=['SCORE','TEAM_NAME'],
                           descending=[True,False]
                      )
    )
    print(f'scores achieved by each team:\n{teams_rank.collect()}')
    
finally:
    conn.close()
