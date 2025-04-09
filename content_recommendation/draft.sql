with recommendations (
  user_id,
  recommendation 
) AS (
        SELECT
            F.USER_ID,
            LI.PAGE_LIKES
        FROM
                 FRIENDS_P6 F
            INNER JOIN LIKES_P6 LI ON F.FRIEND = LI.USER_ID
  ), user_likes (
        user_id,
        recommendation,
        is_matched
  ) AS (
        SELECT
            r.USER_ID,
            r.RECOMMENDATION,
            LI.PAGE_LIKES
        FROM
            recommendations r
            LEFT JOIN LIKES_P6         LI ON r.USER_ID = LI.USER_ID
                                     AND r.RECOMMENDATION = LI.PAGE_LIKES
  )
SELECT DISTINCT
    USER_ID,
    RECOMMENDATION
FROM
    RECOMMENDATION
WHERE
    PAGE_LIKES IS NULL
ORDER BY
    USER_ID;
