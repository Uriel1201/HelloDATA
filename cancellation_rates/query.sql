/* 
01. Cancellation Rates.

Writing a query to return the publication and cancellation 
rate for each user. */

select * from users_p1; 


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

select * from totals_p1;

select user_id, 1.0*publishes/starts as publish_rate, 1.0*cancels/starts as cancel_rate
    from totals_p1;
