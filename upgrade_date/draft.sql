/* 
08. Upgrade Rate by Product Action

Returning the fraction of users, rounded to two
decimal places, who first accessed feature
two (type: F2 in events table) and
upgraded to premium within the first 30
days of signing up. */

/* Querying original data.*/
SELECT
    *
FROM
    USERS_P8;

WITH F2_USERS (
    USER_ID,
    JOIN_DATE
) AS (
    SELECT
        E.USER_ID,
        U.JOIN_DATE
    FROM
             EVENTS_P8 E
        INNER JOIN USERS_P8 U ON E.USER_ID = U.USER_ID
    WHERE
        E.TYPE = 'F2'
), P_USERS (
    USER_ID,
    ACCESS_DATE
) AS (
    SELECT
        USER_ID,
        ACCESS_DATE
    FROM
        EVENTS_P8
    WHERE
        TYPE = 'P'
)
SELECT
    ROUND(
        AVG(
            CASE
                WHEN P.ACCESS_DATE - F.JOIN_DATE <= 30 THEN
                    1
                ELSE 0
            END
        ),
        2
    ) AS UPGRADE_RATE
FROM
    F2_USERS F
    LEFT JOIN P_USERS  P ON F.USER_ID = P.USER_ID;
