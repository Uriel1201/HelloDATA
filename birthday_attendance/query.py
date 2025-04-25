# python -m pip install oracledb numpy pandas
import pandas as pd
import numpy as np
import oracledb

try:
    conn = oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    import oracledb

conn = oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
with conn.cursor() as cur:
   cur.execute("SELECT 'Hello World!' FROM dual")
   res = cur.fetchall()
   print(res)

    table1="SELECT * FROM ATTENDANCE_P11"
    table2="SELECT * FROM STUDENTS_P11"
    attendance=pd.read_sql(table1,conn)
    students=pd.read_sql(table2,conn)
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
    print(f'fraction of birthday attendance:\n{round(fraction,2)}
                          
except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    if 'conn' in locals():
        conn.dispose()
