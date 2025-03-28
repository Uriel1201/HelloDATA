/*
02. Changes in Net Worth

Writing a query to return the change in net worth for each user, 
ordered by decreasing net change. */


/* Querying original data*/
SELECT * FROM TRANSACTIONS_P2; 


/* Table of Senders and their sended amounts*/
CREATE TABLE SENDERS_P2
    AS
        SELECT
            SENDER,
            SUM(AMOUNT) AS SENDING
        FROM
            TRANSACTIONS_P2
        GROUP BY
            SENDER;


/* Table of receivers and their received amounts*/
CREATE TABLE RECEIVERS_P2
    AS
        SELECT
            RECEIVER,
            SUM(AMOUNT) AS RECEIVING
        FROM
            TRANSACTIONS_P2
        GROUP BY
            RECEIVER;


/* Querying senders*/
SELECT
    *
FROM
    SENDERS_P2;


/* Querying receivers*/
SELECT
    *
FROM
    RECEIVERS_P2;


/* Querying the net change of each user*/
SELECT
    COALESCE(S.SENDER, R.RECEIVER)                    AS USER_ID,
    COALESCE(R.RECEIVING, 0) - COALESCE(S.SENDING, 0) AS NET_CHANGE
FROM
    RECEIVERS_P2 R
    FULL OUTER JOIN SENDERS_P2   S ON R.RECEIVER = S.SENDER
ORDER BY
    2 DESC;
