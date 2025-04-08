/* 
01. Cancellation Rates.

Writing a query to return the publication and cancellation 
rate for each user. */


SELECT
    *
FROM
    USERS_P1;


CREATE TABLE TOTALS_P1
    AS
        SELECT
            USER_ID,
            SUM(
                CASE
                    WHEN ACTION = 'start' THEN
                        1
                    ELSE
                        0
                END
            ) AS STARTS,
            SUM(
                CASE
                    WHEN ACTION = 'cancel' THEN
                        1
                    ELSE
                        0
                END
            ) AS CANCELS,
            SUM(
                CASE
                    WHEN ACTION = 'publish' THEN
                        1
                    ELSE
                        0
                END
            ) AS PUBLISHES
        FROM
            USERS_P1
        GROUP BY
            USER_ID
        ORDER BY
            USER_ID;



with TOTALS_P1(user_id, starts, cancels, publishes)
    AS(
        SELECT
            USER_ID,
            SUM(
                CASE
                    WHEN ACTION = 'start' THEN
                        1
                    ELSE
                        0
                END
            ) AS STARTS,
            SUM(
                CASE
                    WHEN ACTION = 'cancel' THEN
                        1
                    ELSE
                        0
                END
            ) AS CANCELS,
            SUM(
                CASE
                    WHEN ACTION = 'publish' THEN
                        1
                    ELSE
                        0
                END
            ) AS PUBLISHES
        FROM
            USERS_P1
        GROUP BY
            USER_ID
        ORDER BY
            USER_ID);



WITH TOTALS_P1 (

SELECT
    USER_ID,
    1.0 * PUBLISHES / STARTS AS PUBLISH_RATE,
    1.0 * CANCELS / STARTS   AS CANCEL_RATE
FROM
    TOTALS_P1;
