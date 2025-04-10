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
    table1 = """SELECT * FROM FRIENDS_P6;"""
    table2 = """SELECT * FROM LIKES_P6;"""
    
    # Leer datos en un DataFrame de pandas
    friends = pd.read_sql(table1, engine)
    likes = pd.read_sql(table2, engine)
    print(friends)
    print(likes)
    df=(pd.merge(friends
                 ,likes.rename(columns={'user_id':'friend'})
                 ,on='friend'
           )
    )
    recommendations=(pd.merge(df
                              ,likes
                              ,on=['user_id','page_likes']
                              ,how='left'
                              ,indicator=True
                        )
                       .rename(columns={'page_likes':'recommendation'})
                       .query("_merge=='left_only'")[['user_id','recommendation']]
                       .drop_duplicates()
                       .sort_values(by='user_id')
    )
    print(recommendations)

except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    # Opcional: Cerrar la conexión si es necesario, pero SQLAlchemy se encarga de gestionarla generalmente.
    if 'engine' in locals():
        engine.dispose()
