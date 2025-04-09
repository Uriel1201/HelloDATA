/* 
05. Super Users.

A company defines its super users as
those who have made at least two
transactions. 
Writing a query to return, for each user, the
date when they become a super user, ordered by oldest super users first.
Users who are not super users should
also be present in the table. */

/* Querying original data */
SELECT
    *
FROM
    USERS_P5;

/*---------------*/
WITH RANKINGS (
    USER_ID,
    TRANSACTION_DATE,
    RANKED_DATE
) AS (
    SELECT
        USER_ID,
        TRANSACTION_DATE,
        ROW_NUMBER()
        OVER(PARTITION BY USER_ID
             ORDER BY
                 TRANSACTION_DATE
        )
    FROM
        USERS_P5
), USERS ( USER_ID ) AS (
    SELECT DISTINCT
        ( USER_ID )
    FROM
        USERS_P5
), SUPERUSERS (
    USER_ID,
    DATE_AS_SUPER
) AS (
    SELECT
        USER_ID,
        TRANSACTION_DATE
    FROM
        RANKINGS
    WHERE
        RANKED_DATE = 2
)
SELECT
    U.USER_ID,
    S.DATE_AS_SUPER
FROM
    USERS      U
    LEFT JOIN SUPERUSERS S ON U.USER_ID = S.USER_ID
ORDER BY
    2;
