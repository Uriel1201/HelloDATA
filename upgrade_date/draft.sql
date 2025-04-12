with F2_USERS(
    user_id,
    join_date
) as (SELECT
    E.USER_ID,
    U.JOIN_DATE
FROM
         EVENTS_P8 E
    INNER JOIN USERS_P8 U ON E.USER_ID = U.USER_ID
WHERE
    E.TYPE = 'F2')
/*------------------------*/
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
    JOIN_DATE,
    ACCESS_DATE
) AS (
    SELECT
        F.USER_ID,
        F.JOIN_DATE,
        E.ACCESS_DATE
    FROM
        F2_USERS  F
        LEFT JOIN EVENTS_P8 E ON F.USER_ID = E.USER_ID
    WHERE
        E.TYPE = 'P'
)
SELECT
    *
FROM
    P_USERS;
