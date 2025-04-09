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
    engine=sqlalchemy.create_engine("oracle+cx_oracle://usr:pswd@localhost/?service_name=orclpdb1", arraysize=1000)

    # Consulta SQL para obtener los datos
    table="""SELECT * FROM ITEMS_P3;"""
    
    # Leer datos en un DataFrame de pandas
    items=pd.read_sql(table, engine)
    print(items)

    # m_f_i: "Most Frequent Item On Each Date"
    f_1=lambda x:x.eq(x.max(axis=1),axis=0)
    m_f_i=(pd.get_dummies(items['Item'])
             .groupby(items['Dates'])
             .sum()
             .pipe(f_1)
             .reset_index()
             .melt(id_vars=['Dates']
                   ,var_name='Item'
                   ,value_name='Was_Top'
                  )
             .sort_values(by='Dates')
          )
    m_f_i[m_f_i['Was_Top']][['Dates','Item']]
     

except SQLAlchemyError as e:
    print(f"Error al conectar a la base de datos o al ejecutar la consulta: {e}")

finally:
    # Opcional: Cerrar la conexión si es necesario, pero SQLAlchemy se encarga de gestionarla generalmente.
    if 'engine' in locals():
        engine.dispose()
