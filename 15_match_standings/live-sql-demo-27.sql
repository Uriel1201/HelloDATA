WITH HOSTS (
    TEAM_ID,
    SCORE
) AS (
    SELECT
        HOST_TEAM,
        SUM(
            CASE
                WHEN HOST_GOALS > GUEST_GOALS THEN
                    3
                WHEN HOST_GOALS = GUEST_GOALS THEN
                    1
                ELSE
                    0
            END
        )
    FROM
        MATCHES_P15
    GROUP BY
        HOST_TEAM
), GUESTS (
    TEAM_ID,
    SCORE
) AS (
    SELECT
        GUEST_TEAM,
        SUM(
            CASE
                WHEN HOST_GOALS > GUEST_GOALS THEN
                    0
                WHEN HOST_GOALS = GUEST_GOALS THEN
                    1
                ELSE
                    3
            END
        )
    FROM
        MATCHES_P15
    GROUP BY
        GUEST_TEAM
)
SELECT
    T.TEAM_NAME,
    H.SCORE + G.SCORE AS SCORE
FROM
    TEAMS_P15 T
    LEFT JOIN HOSTS     H ON T.TEAM_ID = H.TEAM_ID
    LEFT JOIN GUESTS    G ON T.TEAM_ID = G.TEAM_ID
ORDER BY
    2 DESC,
    1;