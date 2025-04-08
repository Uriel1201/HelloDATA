/*
02. Changes in Net Worth

Writing a query to return the change in net worth for each user, 
ordered by decreasing net change. */


/* Querying original data*/
SELECT * FROM TRANSACTIONS_P2; 


/* Querying the net change of each user*/
WITH SENDERS_P2 (
    SENDER,
    SENDING
) AS (
    SELECT
        SENDER,
        SUM(AMOUNT)
    FROM
        TRANSACTIONS_P2
    GROUP BY
        SENDER
), RECEIVERS_P2 (
    RECEIVER,
    RECEIVING
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
    COALESCE(R.RECEIVING, 0) - COALESCE(S.SENDING, 0) AS NET_CHANGE
FROM
    RECEIVERS_P2 R
    FULL OUTER JOIN SENDERS_P2   S ON R.RECEIVER = S.SENDER
ORDER BY
    2 DESC;







/* Querying the net change of each user*/
WITH SENDERS_P2 
/* Table of Senders and their sended amounts*/ (
    SENDER,
    SENDING
) AS (
    SELECT
        SENDER,
        SUM(AMOUNT)
    FROM
        TRANSACTIONS_P2
    GROUP BY
        SENDER
), RECEIVERS_P2 /* Table of receivers and their received amounts*/ (
    RECEIVER,
    RECEIVING
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
    COALESCE(R.RECEIVING, 0) - COALESCE(S.SENDING, 0) AS NET_CHANGE
FROM
    RECEIVERS_P2 R
    FULL OUTER JOIN SENDERS_P2   S ON R.RECEIVER = S.SENDER
ORDER BY
    2 DESC;
