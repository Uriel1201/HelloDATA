with total_scores(hacker_id, total) as (
SELECT
    HACKER_ID,
    SUM(MAX_SCORE) AS SUMA
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
    HACKER_ID)

