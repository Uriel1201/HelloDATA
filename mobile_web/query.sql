/* 
07. Mobile and Web Visitors.

Returning the
fraction of users who only visited
mobile, only visited web, and visited
both. */

/* Querying original data*/
SELECT
    *
FROM
    MOBILE_P7;
SELECT
    *
FROM
    WEB_P7;

/* Querying fraction of users visiting web or mobile*/
WITH USERS ( -- Querying mobile and web users
    MOBILE_USER,
    WEB_USER
) AS (
    SELECT DISTINCT
        M.USER_ID,
        W.USER_ID
    FROM
        MOBILE_P7 M
        FULL OUTER JOIN WEB_P7    W ON M.USER_ID = W.USER_ID
)
SELECT
    AVG(
        CASE
            WHEN MOBILE_USER IS NOT NULL
                 AND WEB_USER IS NULL THEN
                1
            ELSE
                0
        END
    ) AS ARE_MOBILE,
    AVG(
        CASE
            WHEN MOBILE_USER IS NULL
                 AND WEB_USER IS NOT NULL THEN
                1
            ELSE
                0
        END
    ) AS ARE_WEB,
    AVG(
        CASE
            WHEN MOBILE_USER IS NOT NULL
                 AND WEB_USER IS NOT NULL THEN
                1
            ELSE
                0
        END
    ) AS ARE_BOTH
FROM
    USERS;
