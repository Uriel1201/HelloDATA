/*  
06. Content Recommendations

Using the following two tables, write a
query to return page recommendations
to a social media user based on the
pages that their friends have liked, but
that they have not yet marked as liked.
Order the result by ascending user ID. */

/* ORACLE. */

/********************************************************************/
-- Querying the original data
SELECT
    *
FROM
    FRIENDS_P6;

SELECT
    *
FROM
    LIKES_P6;

WITH RECOMMENDATIONS ( -- identifying possible recommendations for each user
    USER_ID,
    RECOMMENDATION
) AS (
    SELECT
        F.USER_ID,
        LI.PAGE_LIKES
    FROM
             FRIENDS_P6 F
        INNER JOIN LIKES_P6 LI ON F.FRIEND = LI.USER_ID
), USER_LIKES ( -- identifying recommendations that have already been marked as liked
    USER_ID,
    RECOMMENDATION,
    IS_MATCHED
) AS (
    SELECT
        R.USER_ID,
        R.RECOMMENDATION,
        LI.PAGE_LIKES
    FROM
        RECOMMENDATIONS R
        LEFT JOIN LIKES_P6        LI ON R.USER_ID = LI.USER_ID
                                 AND R.RECOMMENDATION = LI.PAGE_LIKES
)
SELECT DISTINCT
    USER_ID,
    RECOMMENDATION
FROM
    USER_LIKES
WHERE
    IS_MATCHED IS NULL
ORDER BY
    USER_ID;

/* DUCKDB. */

/********************************************************************/
-- SQLite table friends_06
SELECT
    *
FROM
    'arrow_friends' -- arrow_friends is an arrow table
USING 
    SAMPLE 50% (bernoulli);

-- SQLite table likes_06
SELECT
    *
FROM
    'arrow_likes' -- arrow_likes is an arrow table
USING 
    SAMPLE 50% (bernoulli);

WITH
    RECOMMENDATIONS AS (
        SELECT
            F.USER_ID,
            L.PAGE_LIKES AS RECOMMENDATION
        FROM
            'arrow_friends' F
            INNER JOIN 
                'arrow_likes' L
            ON 
                F.FRIEND = L.USER_ID)
SELECT DISTINCT
    R.USER_ID,
    R.RECOMMENDATION
FROM
    RECOMMENDATIONS R
    ANTI JOIN 
        'arrow_likes' L
    ON
        R.USER_ID = L.USER_ID
    AND
        R.RECOMMENDATION = L.PAGE_LIKES
ORDER BY
    1, 2;
