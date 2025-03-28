/*
02. Changes in Net Worth

Writing a query to return the change in net worth for each user, 
ordered by decreasing net change. */

SELECT * FROM TRANSACTIONS_P2; 


CREATE TABLE SENDINGS_P2
    AS
        SELECT
            SENDER,
            SUM(AMOUNT) AS SENDING
        FROM
            TRANSACTIONS_P2
        GROUP BY
            SENDER;


CREATE TABLE RECEIVERS_P2
    AS
        SELECT
            RECEIVER,
            SUM(AMOUNT) AS RECEIVING
        FROM
            TRANSACTIONS_P2
        GROUP BY
            RECEIVER;

SELECT
    *
FROM
    SENDINGS_P2;
