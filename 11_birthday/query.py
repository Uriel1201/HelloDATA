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
    birthday=(attendance.select(pl.col('student_id')
                                ,pl.col('attendance')
                                ,pl.col('school_date')
                         )
                        .with_columns(birthday_day=pl.col('school_date')
                                                     .dt
                                                     .day(),
                                      birthday_month=pl.col('school_date')
                                                       .dt
                                                       .month()
                         )
                        .join(students.select(pl.col('student_id')
                                              ,pl.col('date_birth')
                                       )
                                      .with_columns(birthday_day=pl.col('date_birth')
                                                                   .dt
                                                                   .day(),
                                                    birthday_month=pl.col('date_birth')
                                                                     .dt
                                                                     .month()
                                       ),
                               on=['student_id','birthday_day','birthday_month'],
                               how='right'
                         )
                        .select(pl.col('student_id')
                                ,pl.col('attendance')
                         )
    ).collect()
    fraction=(birthday.select(pl.col('attendance')
                                .mean()
                                .round(2)
                       )
    )
    print(f'{birthday.head(5)}\nfraction of attendances in birthday dates:\n{fraction}')
     
finally:
    conn.close()
