/* 
09. Most Friended.

Returning a list of
users and their corresponding friend
count. Assuming that only
unique friendships are displayed. */

WITH FRIENDSHIP ( FRIEND_ID ) AS (
    SELECT
        USER_1
    FROM
        FRIENDS_P9
    UNION ALL
    SELECT
        USER_2
    FROM
        FRIENDS_P9
)
SELECT
    FRIEND_ID,
    COUNT(*) AS NUMBER_OF_FRIENDS
FROM
    FRIENDSHIP
GROUP BY
    FRIEND_ID
ORDER BY
    2 DESC,
    1;
