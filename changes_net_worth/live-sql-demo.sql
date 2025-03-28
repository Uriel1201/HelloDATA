CREATE TABLE sendings_p2 as 
   SELECT sender, SUM(amount) as sendings_p2 
      FROM TRANSACTIONS_P2
      GROUP BY sender;