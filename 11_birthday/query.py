# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table1="SELECT * FROM ATTENDANCE_P11"
    table2="SELECT * FROM STUDENTS_P11"
    odf1=conn.fetch_df_all(statement=table1,arraysize=100)
    odf2=conn.fetch_df_all(statement=table2,arraysize=100)
    pyarrow_table1=pyarrow.Table.from_arrays(odf1.column_arrays(),names=odf1.column_names())
    pyarrow_table2=pyarrow.Table.from_arrays(odf2.column_arrays(),names=odf2.column_names())
    attendance=pl.from_arrow(pyarrow_table1).lazy()
    students=pl.from_arrow(pyarrow_table2).lazy()
    birthday=(attendance.select(pl.col('STUDENT_ID')
                                ,pl.col('ATTENDANCE')
                                ,pl.col('SCHOOL_DATE')
                         )
                        .with_columns(birthday_day=pl.col('SCHOOL_DATE')
                                                     .dt
                                                     .day(),
                                      birthday_month=pl.col('SCHOOL_DATE')
                                                       .dt
                                                       .month()
                         )
                        .join(students.select(pl.col('STUDENT_ID')
                                              ,pl.col('DATE_BIRTH')
                                       )
                                      .with_columns(BIRTHDAY_DAY=pl.col('DATE_BIRTH')
                                                                   .dt
                                                                   .day(),
                                                    BIRTHDAY_MONTH=pl.col('DATE_BIRTH')
                                                                     .dt
                                                                     .month()
                                       ),
                               on=['STUDENT_ID','BIRTHDAY_DAY','BIRTHDAY_MONTH'],
                               how='right'
                         )
                        .select(pl.col('STUDENT_ID')
                                ,pl.col('ATTENDANCE')
                         )
    ).collect()
    fraction=(birthday.select(pl.col('ATTENDANCE')
                                .mean()
                                .round(2)
                       )
    )
    print(f'{birthday.head(5)}\nfraction of attendances in birthday dates:\n{fraction}')
     
finally:
    conn.close()
