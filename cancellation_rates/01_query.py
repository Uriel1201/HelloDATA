# %%
# !! {"metadata":{
# !!   "id": "2b2hu8zjIJQW"
# !! }}
"""
# 01. Cancelation Rates
*From the following table of user IDs, actions, and dates, write a query to       return the publication and cancellation rate for each user.*
"""

# %%
# !! {"metadata":{
# !!   "id": "sELZZX6eILuD",
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/",
# !!     "height": 226
# !!   },
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1742988406025,
# !!     "user_tz": 360,
# !!     "elapsed": 513,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "d7d2f43e-15cc-489c-874c-d8b4590206da"
# !! }}
# pip install pandas
# pip install numpy
# pip install SQLAlchemy
# pip install cx_Oracle

import pandas as pd
import numpy  as np
# import cx_Oracle
# import sqlalchemy
# from sqlalchemy.exc import SQLAlchemyError

'''
01. Cancellation Rates.

Writing a query to return the publication and cancellation
rate for each user


try:
  engine = sqlalchemy.create_engine("oracle+cx_oracle://usr:pswd@localhost/?service_name=orclpdb1", arraysize=1000)

  table = """select * from users_p1""";
  users = pd.read_sql(table, engine)
  users

except SQLAlchemyError as e:
  print(e)
'''

# %%
# !! {"metadata":{
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/",
# !!     "height": 488
# !!   },
# !!   "id": "fFsYJ2qCI086",
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1742989395548,
# !!     "user_tz": 360,
# !!     "elapsed": 168,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "e68c9221-8a80-456d-dbaf-ce21ff950ccb"
# !! }}
data = {'user_id' :[1,1,2,1,1,2,3,3,4],
        'action'  :['start','cancel','start',
                    'start','publish','publish',
                    'start','cancel','start'],
        'dates'   :['01-JAN-20',
                    '02-JAN-20',
                    '03-JAN-20',
                    '03-JAN-20',
                    '04-JAN-20',
                    '04-JAN-20',
                    '05-JAN-20',
                    '06-JAN-20',
                    '07-JAN-20']
        }

users = pd.DataFrame(data)
users

# %%
# !! {"metadata":{
# !!   "id": "n4vwCAiPQ5D_"
# !! }}
actions = (pd.get_dummies(users['action'])
          .groupby(users['user_id'])
          .sum()
          .assign(publish_rate = lambda x : x['publish'] / x['start'],
                  cancel_rate = lambda x : x['cancel'] / x['start']
                 )
          .replace(np.inf, 0)
          .reset_index()
          )

# %%
# !! {"metadata":{
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/",
# !!     "height": 195
# !!   },
# !!   "id": "xVdh_O29RJQh",
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1742992368869,
# !!     "user_tz": 360,
# !!     "elapsed": 16,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "496dcd1b-4698-457f-ba00-a5bee620673a"
# !! }}
actions[['user_id', 'publish_rate', 'cancel_rate']]

# %%
# !! {"main_metadata":{
# !!   "colab": {
# !!     "provenance": [],
# !!     "authorship_tag": "ABX9TyOs+hJVb2is8RdMi/ENpQE9"
# !!   },
# !!   "kernelspec": {
# !!     "name": "python3",
# !!     "display_name": "Python 3"
# !!   },
# !!   "language_info": {
# !!     "name": "python"
# !!   }
# !! }}
