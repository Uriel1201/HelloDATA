# %%
# !! {"metadata":{
# !!   "id": "IaOAIx-C0p3t"
# !! }}
"""
# 2. Changes in Net Worth
*From the following table of transactions between two users, write a query to return the change in net worth for each user, ordered by decreasing net change.*
"""

# %%
# !! {"metadata":{
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/",
# !!     "height": 192
# !!   },
# !!   "id": "HojUKQ-X0xjq",
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1743163818919,
# !!     "user_tz": 360,
# !!     "elapsed": 23,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "a245d2a0-2ef3-4aa7-b9fe-3214a3efff13"
# !! }}
# pip install SQLAlchemy
# pip install cx_Oracle

import pandas as pd
import numpy  as np
# import cx_Oracle
# import sqlalchemy
# from sqlalchemy.exc import SQLAlchemyError

'''
try:
  engine = sqlalchemy.create_engine("oracle+cx_oracle://usr:pswd@localhost/?service_name=orclpdb1", arraysize=1000)

  table = """select * from transactions_p2""";
  transactions = pd.read_sql(table, engine)
  transactions

except SQLAlchemyError as e:
  print(e)
'''

# %%
# !! {"metadata":{
# !!   "id": "84LsWnzx1N8E",
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1743163822179,
# !!     "user_tz": 360,
# !!     "elapsed": 12,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "cfa3def1-f387-416b-e01d-ab07a7190083",
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/"
# !!   }
# !! }}
data = {'Sender'            : [5, 1, 2, 2, 3, 3, 1],
        'Receiver'          : [2, 3, 1, 3, 1, 2, 4],
        'Amount'            : [10, 15, 20, 25, 20, 15, 5],
        'Transaction_Date'  : ['12-FEB-20',
                               '13-FEB-20',
                               '13-FEB-20',
                               '14-FEB-20',
                               '15-FEB-20',
                               '15-FEB-20',
                               '16-FEB-20']
        }

transactions = pd.DataFrame(data)
transactions['Transaction_Date'] = pd.to_datetime(transactions['Transaction_Date'])
print(transactions)

# %%
# !! {"metadata":{
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/"
# !!   },
# !!   "id": "pllke1fib0Go",
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1743163829837,
# !!     "user_tz": 360,
# !!     "elapsed": 8,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "6fc8ecc1-7971-443e-ec34-807a58b29807"
# !! }}
df1 = (transactions.melt(id_vars=['Amount']
                         ,value_vars=['Sender', 'Receiver']
                         ,var_name='Type'
                         ,value_name='User_id'
                    )
)
print(df1)

# %%
# !! {"metadata":{
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/"
# !!   },
# !!   "id": "UVQS85Ocg9ys",
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1743163836656,
# !!     "user_tz": 360,
# !!     "elapsed": 9,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "5f94dc9b-87e6-4153-d209-101f8e718b49"
# !! }}
df2 = (transactions.melt(id_vars=['Amount']
                         ,value_vars=['Sender', 'Receiver']
                         ,var_name='Type'
                         ,value_name='User_id'
                    )
                   .assign(Amount=lambda x:
                                         x['Amount'] * x['Type'].map({'Sender': -1, 'Receiver': 1})
                    )
)
print(df2)

# %%
# !! {"metadata":{
# !!   "colab": {
# !!     "base_uri": "https://localhost:8080/",
# !!     "height": 206
# !!   },
# !!   "id": "5N2_paqvmqZw",
# !!   "executionInfo": {
# !!     "status": "ok",
# !!     "timestamp": 1743164119529,
# !!     "user_tz": 360,
# !!     "elapsed": 24,
# !!     "user": {
# !!       "displayName": "Uriel Garc\u00eda",
# !!       "userId": "03386744220426758265"
# !!     }
# !!   },
# !!   "outputId": "e38b5409-46c1-4882-c50d-d2d912dd52da"
# !! }}
net_changes = (transactions.melt(id_vars=['Amount']
                                 ,value_vars=['Sender', 'Receiver']
                                 ,var_name='Type'
                                 ,value_name='User_id'
                            )
                           .assign(Amount=lambda x:
                                                 x['Amount'] * x['Type'].map({'Sender': -1, 'Receiver': 1})
                            )
                           .groupby('User_id')['Amount']
                           .sum()
                           .reset_index(name='Net_Changes'
                            )
                           .sort_values(by='Net_Changes'
                                        , ascending=False
                            )
)
net_changes

# %%
# !! {"main_metadata":{
# !!   "colab": {
# !!     "provenance": [],
# !!     "authorship_tag": "ABX9TyP6w9lGcdHrT5z2Rncv/E9W"
# !!   },
# !!   "kernelspec": {
# !!     "name": "python3",
# !!     "display_name": "Python 3"
# !!   },
# !!   "language_info": {
# !!     "name": "python"
# !!   }
# !! }}
