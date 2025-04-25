# python -m pip install oracledb numpy pandas pyarrow --upgrade
import pandas as pd
import numpy as np
import oracledb
import pyarrow 

try:
    conn=oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    table1="SELECT * FROM ATTENDANCE_P11"
    table2="SELECT * FROM STUDENTS_P11"
    odf_attendance=conn.fetch_df_all(statement=table1,arraysize=100)
    odf_students=conn.fetch_df_all(statement=table2,arraysize=100)
    attendance=pyarrow.Table.from_arrays(odf_attendance.column_arrays(),names=odf_attendance.column_names()).to_pandas()
    students=pyarrow.Table.from_arrays(odf_students.column_arrays(),names=odf_students.column_names()).to_pandas()
    
    attendance['school_date']=pd.to_datetime(attendance['school_date'],format="%d-%b-%y")
    students['date_birth']=pd.to_datetime(students['date_birth'],format="%d-%b-%y")
    
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
