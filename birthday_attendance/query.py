# pip install pandas
# pip install numpy
python -m pip install oracledb
import pandas as pd
import numpy as np
import oracledb

conn = oracledb.connect(user="[Username]", password="[Password]", dsn="localhost:1521/FREEPDB1")
with conn.cursor() as cur:
   cur.execute("SELECT 'Hello World!' FROM dual")
   res = cur.fetchall()
   print(res)
try:
    # Cadena de conexión a la base de datos (reemplaza con tus propios detalles)
    engine=sqlalchemy.create_engine("oracle+cx_oracle://usr:pswd@localhost/?service_name=orclpdb1", arraysize=1000)

    # Consulta SQL para obtener los datos
    table="""SELECT * FROM PROJECTS_P10;"""
    
    # Leer datos en un DataFrame de pandas
    projects=pd.read_sql(table,engine)
                          
except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    # Opcional: Cerrar la conexión si es necesario, pero SQLAlchemy se encarga de gestionarla generalmente.
    if 'engine' in locals():
        engine.dispose()
