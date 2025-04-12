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
), p_users(
    user_id,
    Access_date
) as (
    select user_id, access_date
    from events_p8
    where type='P'
)  SELECT
        F.USER_ID,
        E.ACCESS_DATE-f.join_date
    FROM
        F2_USERS  F
        LEFT JOIN EVENTS_P8 E ON F.USER_ID = E.USER_ID
)
SELECT
    *
FROM
    P_USERS;
/*-----------------------*/
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
), P_UPGRADE (
    USER_ID,
    UPGRADE_TIME
) AS (
    SELECT
        F.USER_ID,
        E.ACCESS_DATE - F.JOIN_DATE
    FROM
        F2_USERS  F
        LEFT JOIN EVENTS_P8 E ON F.USER_ID = E.USER_ID
    WHERE
        E.TYPE = 'P'
)
SELECT
    *
FROM
    P_UPGRADE;
