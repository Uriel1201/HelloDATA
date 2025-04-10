'''
Using the following two tables, write a query to return page recommendations 
to a social media user based on the pages that their friends have liked,
but that they have not yet marked as liked.
Order the result by ascending user ID.
'''
import pandas as pd
import numpy  as np

data1 = {'user_id': [1,1,1,2,3,3,4,4],
         'friend'  : [2,3,4,1,1,4,1,3]
        }

data2 = {'user_id' : [1,1,1,2,3,3,4],
         'page_likes' : ['A','B','C','A','B','C','B']
        }

friends = pd.DataFrame(data1)
likes   = pd.DataFrame(data2)


## r_p: recommending pages for each user
## _r_p: recommending pages not yet marked as liked 
df=likes.rename(columns={'user_id':'friend'})
r_p=(pd.merge(friends,df,on='friend',how='inner')
       .drop(columns=['friend'])
)
_r_p=(pd.merge(r_p,likes,on=['user_id','page_likes'],how='left',indicator=True)
        .rename(columns={'page_likes':'recommendation'})
)
(_r_p[['user_id','recommendation']][_r_p['_merge']=='left_only'].drop_duplicates()
                                                                .sort_values(by='user_id')
)
