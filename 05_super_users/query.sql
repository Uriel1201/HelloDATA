/* 
05. Super Users.

A company defines its super users as
those who have made at least two
transactions. 
Writing a query to return, for each user, the
date when they become a super user, ordered by oldest super users first.
Users who are not super users should
also be present in the table. */

/* ORACLE. 
   (HTAP)*/

/********************************************************************/
-- Querying original data 
SELECT
    *
FROM
    USERS_P5;

                -- Querying dates when users become superusers
WITH RANKINGS ( -- identifying superuser dates
    USER_ID,
    TRANSACTION_DATE,
    RANKED_DATE
) AS (
    SELECT
        USER_ID,
        TRANSACTION_DATE,
        ROW_NUMBER()
        OVER(PARTITION BY USER_ID
             ORDER BY
                 TRANSACTION_DATE
        )
    FROM
        USERS_P5
), USERS ( -- identifying unique users 
 USER_ID ) AS (
    SELECT DISTINCT
        ( USER_ID )
    FROM
        USERS_P5
), SUPERUSERS ( -- identifying superusers 
    USER_ID,
    DATE_AS_SUPER
) AS (
    SELECT
        USER_ID,
        TRANSACTION_DATE
    FROM
        RANKINGS
    WHERE
        RANKED_DATE = 2
)
SELECT
    U.USER_ID,
    S.DATE_AS_SUPER
FROM
    USERS      U
    LEFT JOIN SUPERUSERS S ON U.USER_ID = S.USER_ID
ORDER BY
    2;

/* DUCKDB. 
   (OLAP)*/

/********************************************************************/
SELECT
    *
FROM
    USERS;

WITH 
    RANKINGS AS (
        SELECT
            USER_ID,
            TRANSACTION_DATE,
            ROW_NUMBER() OVER(PARTITION BY 
                                  USER_ID
                              ORDER BY
                                  TRANSACTION_DATE) AS RANKED_DATE
        FROM
            USERS), 
    USER_ AS (
        SELECT DISTINCT
            USER_ID
        FROM
            USERS), 
    SUPERUSERS AS (
        SELECT
            USER_ID,
            TRANSACTION_DATE AS DATE_AS_SUPER
        FROM
            RANKINGS
        WHERE
            RANKED_DATE = 2)
SELECT
    U.USER_ID,
    S.DATE_AS_SUPER
FROM
    USER_      U
    LEFT JOIN 
        SUPERUSERS S 
    USING (USER_ID)
ORDER BY
    2;
