/***********************************/
WITH TOTAL_SCORES (
    HACKER_ID,
    SCORE
) AS (
    SELECT
        HACKER_ID,
        SUM(MAX_SCORE)
    FROM
        (
            SELECT
                HACKER_ID,
                CHALLENGE_ID,
                MAX(SCORE) AS MAX_SCORE
            FROM
                SUBMISSIONS_P12
            GROUP BY
                HACKER_ID,
                CHALLENGE_ID
        )
    GROUP BY
        HACKER_ID
)
SELECT
    H.HACKER_ID,
    H.NAME,
    T.SCORE
FROM
    HACKERS_P12  H
    LEFT JOIN TOTAL_SCORES T ON H.HACKER_ID = T.HACKER_ID
WHERE
    T.SCORE > 0
ORDER BY
    3 DESC,
    1;
