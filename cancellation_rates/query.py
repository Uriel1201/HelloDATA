"""
# 01. Cancelation Rates
*From the following table of user IDs, actions, and dates, 
write a query to return the publication and cancellation rate 
for each user.*
"""


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
'''

try:
  engine = sqlalchemy.create_engine("oracle+cx_oracle://usr:pswd@localhost/?service_name=orclpdb1", arraysize=1000)

  table = """select * from users_p1""";
  users=pd.read_sql(table, engine)
  users
  actions=(pd.get_dummies(users['action'])
             .groupby(users['user_id'])
             .sum()
             .assign(publish_rate = lambda x: 
                                           x['publish'] / x['start'],
                     cancel_rate = lambda x: 
                                          x['cancel'] / x['start']
              )
             .replace(np.inf, 0)
             .reset_index()
  )
  print(actions[['user_id', 'publish_rate', 'cancel_rate']])

except SQLAlchemyError as e:
  print(e)


