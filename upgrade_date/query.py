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
    table1 = """SELECT * FROM USERS_P8;"""
    table2 = """SELECT * FROM EVENTS_P8;"""
    
    # Leer datos en un DataFrame de pandas
    users = pd.read_sql(table1, engine)
    events = pd.read_sql(table2, engine)
    users['join_date'] = pd.to_datetime(users['join_date'])
    events['access_date'] = pd.to_datetime(events['access_date'])
    
    premium_upgrade=(users.drop(columns=['name'])
                          .merge(events.query("Type=='F2'")[['user_id']]
                                 ,on='user_id'
                                 ,how='inner'
                           )
                          .merge(events.query("Type=='P'")[['user_id','access_date']]
                                 ,on='user_id'
                                 ,how='left'
                           )
    )
    premium_upgrade['WithinFirst30']=premium_upgrade['access_date']-premium_upgrade['join_date']<=pd.Timedelta(days=30)
    print(f'Was upgraded within first 30 days:\n{premium_upgrade}\n')
    upgrade_rate=round(premium_upgrade['WithinFirst30'].mean()
                       ,2
    )
    print(f'upgrade_rate: {upgrade_rate}')
    
except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    # Opcional: Cerrar la conexión si es necesario, pero SQLAlchemy se encarga de gestionarla generalmente.
    if 'engine' in locals():
        engine.dispose()
