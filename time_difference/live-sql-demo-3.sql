/* 
04. Time Difference Between Latest Actions

writing a query to return for each user the
time elapsed between the last action
and the second-to-last action, in
ascending order by user ID. */ 

/* Querying original data*/
SELECT
    *
FROM
    USERS_P4;

        /* Time elapsed between the last action 
        and the second-to-last action */
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