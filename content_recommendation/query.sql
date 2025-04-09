/*  
06. Content Recommendations

Writing a query to return page recommendations
to a social media user based on the
pages that their friends have liked, but
that they have not yet marked as liked. */


SELECT
    *
FROM
    FRIENDS_P6;

SELECT
    *
FROM
    LIKES_P6;

/*----------------------*/
WITH RECOMMENDATIONS (
    USER_ID,
    RECOMMENDATION
) AS (
    SELECT
        F.USER_ID,
        LI.PAGE_LIKES
    FROM
             FRIENDS_P6 F
        INNER JOIN LIKES_P6 LI ON F.FRIEND = LI.USER_ID
), USER_LIKES (
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
