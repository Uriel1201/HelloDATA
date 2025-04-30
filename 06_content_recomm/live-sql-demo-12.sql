/*  
06. Content Recommendations

Writing a query to return page recommendations
to a social media user based on the
pages that their friends have liked, but
that they have not yet marked as liked. */

/* Querying the original data*/
SELECT
    *
FROM
    FRIENDS_P6;

SELECT
    *
FROM
    LIKES_P6;

                                /* Querying pages recommendations for users 
                                                   based on pages liked by their friends */
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