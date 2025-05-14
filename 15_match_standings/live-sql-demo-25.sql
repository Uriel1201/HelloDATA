WITH HOST (
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
), GUEST (
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
)
SELECT
    T.TEAM_ID,
    SUM(H.SCORE + G.SCORE)
FROM
    TEAMS_P15
    LEFT JOIN HOST  H ON T.TEAM_ID = H.TEAM_ID
    LEFT JOIN GUEST G ON T.TEAM_ID = G.TEAM_ID