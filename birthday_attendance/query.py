# pip install pandas
# pip install numpy
python -m pip install oracledb
import pandas as pd
import numpy as np
import oracledb

try:
    conn = oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
    # Consulta SQL para obtener los datos
    table1="SELECT * FROM ATTENDANCE_P11"
    table2="SELECT * FROM STUDENTS_P11"
    attendance=pd.read_sql(table1,conn)
    students=pd.read_sql(table2,conn)
    attendance['school_date']=pd.to_datetime(attendance['school_date'],format="%d-%b-%y")
    students['date_birth']=pd.to_datetime(students['date_birth'],format="%d-%b-%y")
                          
except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    # Opcional: Cerrar la conexión si es necesario, pero SQLAlchemy se encarga de gestionarla generalmente.
    if 'engine' in locals():
        engine.dispose()
