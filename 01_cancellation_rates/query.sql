/* 
01. Cancellation Rates.

From the following table of user IDs,
actions, and dates, write a query to
return the publication and cancellation
rate for each user. */

/* ORACLE. */

/********************************************************************/

-- Querying original data.
SELECT
    *
FROM
    USERS_P1;

        -- Returning rates for each user.
WITH TOTALS ( -- Totals for each action 
    USER_ID,
    TOTAL_STARTS,
    TOTAL_CANCELS,
    TOTAL_PUBLISHES
) AS (
    SELECT
        USER_ID,
        SUM(
            CASE
                WHEN ACTION = 'start' THEN
                    1
                ELSE
                    0
            END
        ),
        SUM(
            CASE
                WHEN ACTION = 'cancel' THEN
                    1
                ELSE
                    0
            END
        ),
        SUM(
            CASE
                WHEN ACTION = 'publish' THEN
                    1
                ELSE
                    0
            END
        )
    FROM
        USERS_P1
    GROUP BY
        USER_ID
)
SELECT
    USER_ID,
    ROUND(TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS, 0),
          2) AS PUBLISH_RATE,
    ROUND(TOTAL_CANCELS / NULLIF(TOTAL_STARTS, 0),
          2) AS CANCEL_RATE
FROM
    TOTALS;

/* DUCKDB. */

/********************************************************************/

SELECT 
   * 
FROM 
   USERS;

WITH TOTALS AS (
    SELECT
        USER_ID,
        SUM(IF(ACTION = 'start',1,0)) AS TOTAL_STARTS,
        SUM(IF(ACTION = 'cancel',1,0)) AS TOTAL_CANCELS,
        SUM(IF(ACTION = 'publish',1,0)) AS TOTAL_PUBLISHES
    FROM
        USERS
    GROUP BY
        USER_ID) 
SELECT
    USER_ID,
    ROUND(TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS, 0), 2) AS PUBLISH_RATE,
    ROUND(TOTAL_CANCELS / NULLIF(TOTAL_STARTS, 0), 2) AS CANCEL_RATE
FROM
    TOTALS
ORDER BY 
    1;
