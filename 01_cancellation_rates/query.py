# python -m pip install oracledb numpy pyarrow polars --upgrade
import numpy as np
import polars as pl
import oracledb
import pyarrow

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table="SELECT * FROM USERS_P1"
    odf_users=conn.fetch_df_all(statement=table,arraysize=100)
    attendance=pyarrow.Table.from_arrays(odf_attendance.column_arrays(),names=odf_attendance.column_names()).to_pandas()
    students=pyarrow.Table.from_arrays(odf_students.column_arrays(),names=odf_students.column_names()).to_pandas()
    
    attendance_copy=attendance[['student_id','attendance']].copy()
    attendance_copy['month']=attendance['school_date'].dt.month
    attendance_copy['day']=attendance['school_date'].dt.day
    students_copy=students[['student_id']].copy()
    students_copy['month']=students['date_birth'].dt.month
    students_copy['day']=students['date_birth'].dt.day
    attendance_on_birthday=(pd.merge(students_copy,
                                     attendance_copy,
                                     on=['student_id','month','day'],
                                     how='left'
                               )
    )
    fraction=attendance_on_birthday['attendance'].mean()
    print(f'fraction of birthday attendance:\n{round(fraction,2)}')
                          
finally:
    conn.close()
