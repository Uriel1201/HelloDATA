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
    table1 = """SELECT * MOBILE_P7;"""
    table2 = """SELECT * WEB_P7;"""
    # Leer datos en un DataFrame de pandas
    mobile = pd.read_sql(table1, engine)
    web = pd.read_sql(table2, engine)
    print(mobile)
    print(web)
    
    df=(pd.merge(mobile.drop(columns=['page_url']),
                 web.drop(columns=['page_url']),
                 on='user_id',
                 how='outer',
                 indicator=True
           )
          .drop_duplicates()
    )
    frequencies=(pd.get_dummies(df['_merge'])
                   .rename(columns={'left_only':'mobile'
                                    ,'right_only':'web'
                           }
                    )
                   .mean()
    )
    print('fractions:')
    print(frequencies)

except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    # Opcional: Cerrar la conexión si es necesario, pero SQLAlchemy se encarga de gestionarla generalmente.
    if 'engine' in locals():
        engine.dispose()
