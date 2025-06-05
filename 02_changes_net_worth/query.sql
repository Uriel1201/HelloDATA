/*
02. Changes in Net Worth

From the following table of transactions
between two users, write a query to
return the change in net worth for each
user, ordered by decreasing net change. */

/* ORACLE. */

/********************************************************************/
-- Querying original data
SELECT
    *
FROM
    TRANSACTIONS_P2;

        -- Querying the net change of each user
WITH SENDERS ( -- Table of Senders and their sended amounts
    SENDER,
    SENDED
) AS (
    SELECT
        SENDER,
        SUM(AMOUNT)
    FROM
        TRANSACTIONS_P2
    GROUP BY
        SENDER
), RECEIVERS ( -- Table of receivers and their received amounts
    RECEIVER,
    RECEIVED 
) AS (
    SELECT
        RECEIVER,
        SUM(AMOUNT)
    FROM
        TRANSACTIONS_P2
    GROUP BY
        RECEIVER
)
SELECT
    COALESCE(S.SENDER, R.RECEIVER)                    AS USER_ID,
    COALESCE(R.RECEIVED, 0) - COALESCE(S.SENDED, 0) AS NET_CHANGE
FROM
    RECEIVERS R
    FULL OUTER JOIN SENDERS   S ON R.RECEIVER = S.SENDER
ORDER BY
    2 DESC;

/* DUCKDB. */

/********************************************************************/
SELECT * FROM TRANSACTIONS;

WITH SENDERS AS (SELECT
                     SENDER,
                     SUM(AMOUNT) AS SENDED
                 FROM 
                     TRANSACTIONS
                 GROUP BY
                     SENDER),
RECEIVERS AS (SELECT
                  RECEIVER,
                  SUM(AMOUNT) AS RECEIVED
              FROM
                  TRANSACTIONS
              GROUP BY
                  RECEIVER) SELECT
                                COALESCE(S.SENDER, R.RECEIVER) AS USER_ID,
                                COALESCE(R.RECEIVED, 0) - COALESCE(S.SENDED, 0) AS NET_CHANGE
                            FROM
                                RECEIVERS R
                            FULL JOIN SENDERS S ON (R.RECEIVER = S.SENDER)
                            ORDER BY 2 DESC;
