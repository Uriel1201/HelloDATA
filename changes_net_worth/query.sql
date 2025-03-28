/*
02. Changes in Net Worth

Writing a query to return the change in net worth for each user, 
ordered by decreasing net change. */

SELECT * FROM TRANSACTIONS_P2; 

CREATE TABLE sendings_p2 as 
   SELECT sender, SUM(amount) as sendings_p2 
      FROM TRANSACTIONS_P2
      GROUP BY sender;
