# pip install pandas
# pip install numpy
# pip install SQLAlchemy
# pip install cx_Oracle

import pandas as pd
import numpy as np
import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

try:
    # Cadena de conexión a la base de datos (reemplaza con tus propios detalles)
    engine = sqlalchemy.create_engine("oracle+cx_oracle://usr:pswd@localhost/?service_name=orclpdb1", arraysize=1000)

    # Consulta SQL para obtener los datos
    table = """SELECT * FROM USERS_P5;"""
    
    # Leer datos en un DataFrame de pandas
    users = pd.read_sql(table, engine)
    print(users)

    
    super_users=(users.sort_values(by=['user_id','transaction_date'])
                  .groupby('user_id'
                           ,as_index=False
                   )
                  .agg(super_date=('transaction_date'
                                   ,lambda x:
                                           x.iloc[1] if len(x)>1 else pd.NA
                       )
                   )
    )
    super_users

except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    # Opcional: Cerrar la conexión si es necesario, pero SQLAlchemy se encarga de gestionarla generalmente.
    if 'engine' in locals():
        engine.dispose()
