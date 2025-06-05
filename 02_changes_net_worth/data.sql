/*
02. Changes in Net Worth

From the following table of transactions
between two users, write a query to
return the change in net worth for each
user, ordered by decreasing net change. */

/* ORACLE. */

/********************************************************************/
CREATE TABLE TRANSACTIONS_P2 (
    SENDER           INTEGER,
    RECEIVER         INTEGER,
    AMOUNT           DECIMAL,
    TRANSACTION_DATE DATE
);

INSERT INTO TRANSACTIONS_P2
    WITH NAMES AS (
        SELECT
            5,
            2,
            10,
            '12-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            3,
            15,
            '13-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            1,
            20,
            '13-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            3,
            25,
            '14-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            1,
            20,
            '15-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            2,
            15,
            '15-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            4,
            5,
            '16-feb-20'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

/* DUCKDB. */

/********************************************************************/
CREATE TABLE TRANSACTIONS (SENDER           INTEGER,
                           RECEIVER         INTEGER,
                           AMOUNT           DECIMAL,
                           TRANSACTION_DATE DATE
                          );

INSERT INTO TRANSACTIONS  BY POSITION VALUES (5, 2, 10.0, "2020-02-12"),
                                             (1, 3, 15.0, "2020-02-13"),
                                             (2, 1, 20.0, "2020-02-13"),
                                             (2, 3, 25.0, "2020-02-14"),
                                             (3, 1, 20.0, "2020-02-15"),
                                             (3, 2, 15.0, "2020-02-15"),
                                             (1, 4, 5.0, "2020-02-16");
