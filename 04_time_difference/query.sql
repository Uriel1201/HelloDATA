/* 
04. Time Difference Between Latest Actions

writing a query to return for each user the
time elapsed between the last action
and the second-to-last action, in
ascending order by user ID. */ 

/* ORACLE. */

/********************************************************************/
-- Querying original data
SELECT
    *
FROM
    USERS_P4;

-- Time elapsed between the last action and the second-to-last action
WITH RANKINGS ( -- Ordering dates in descending order grouped by each user
    ID,
    ACTION_DATE,
    RANKED_DATES
) AS (
    SELECT
        ID,
        ACTION_DATE,
        ROW_NUMBER()
        OVER(PARTITION BY ID
             ORDER BY
                 ACTION_DATE DESC
        )
    FROM
        USERS_P4
), RANKED1 ( -- last date recorded for each user
    ID,
    LAST
) AS (
    SELECT
        ID,
        ACTION_DATE
    FROM
        RANKINGS
    WHERE
        RANKED_DATES = 1
), RANKED2 ( -- penultimate date recorded for each user
    ID,
    PENULTIMATE
) AS (
    SELECT
        ID,
        ACTION_DATE
    FROM
        RANKINGS
    WHERE
        RANKED_DATES = 2
)
SELECT
    A.ID,
    ( A.LAST - B.PENULTIMATE ) AS ELAPSED_TIME
FROM
    RANKED1 A
    LEFT JOIN RANKED2 B ON A.ID = B.ID
ORDER BY
    A.ID;

/* DUCKDB. */

/********************************************************************/
SELECT 
    *
FROM
    USERS;

WITH 
    ORDERED_DATES AS (
        SELECT
            ID,
            ACTION_DATE,
            ROW_NUMBER() OVER (PARTITION BY 
                                   ID
                               ORDER BY
                                   ACTION_DATE DESC) AS ORDERED
        FROM
            USERS), 
    LAST_DATES AS (
        SELECT
            ID,
            ACTION_DATE AS LAST_DATE
        FROM
            ORDERED_DATES
        WHERE
            ORDERED = 1), 
    PENULTIMATE_DATES AS (
        SELECT
            ID,
            ACTION_DATE AS PENULTIMATE_DATE
        FROM
            ORDERED_DATES
        WHERE
            ORDERED = 2)
SELECT
    L.ID,
    (L.LAST_DATE - P.PENULTIMATE_DATE) AS ELAPSED_TIME
FROM
    LAST_DATES L
    LEFT JOIN 
        PENULTIMATE_DATES P 
    USING (ID)
ORDER BY
    1;
