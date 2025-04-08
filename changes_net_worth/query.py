"""
# 2. Changes in Net Worth
*From the following table of transactions between two users, 
write a query to return the change in net worth for each user,
ordered by decreasing net change.*
"""


# pip install SQLAlchemy
# pip install cx_Oracle

import pandas as pd
import numpy  as np
# import cx_Oracle
# import sqlalchemy
# from sqlalchemy.exc import SQLAlchemyError

try:
  engine=sqlalchemy.create_engine("oracle+cx_oracle://usr:pswd@localhost/?service_name=orclpdb1", arraysize=1000)
  table="""select * from transactions_p2;"""
  transactions=pd.read_sql(table,engine)
  transactions['Transaction_Date']=pd.to_datetime(transactions['Transaction_Date'])
  print(transactions)
  net_changes=(transactions.melt(id_vars=['Amount']
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
  print(net_changes)
except SQLAlchemyError as e:
  print(e)






