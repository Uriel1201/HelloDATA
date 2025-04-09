/* 
01. Cancellation Rates.

Writing a query to return the publication and cancellation 
rate for each user. */


SELECT
    *
FROM
    USERS_P1;


WITH TOTALS (
    USER_ID,
    STARTS,
    CANCELS,
    PUBLISHES
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
    ROUND(PUBLISHES / NULLIF(STARTS, 0),
          2) AS PUBLISH_RATE,
    ROUND(CANCELS / NULLIF(STARTS, 0),
          2) AS CANCEL_RATE
FROM
    TOTALS;
