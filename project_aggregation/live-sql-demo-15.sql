WITH SORTED ( -- ordering dates
    S_D,
    E_D,
    PREV_END
) AS (
    SELECT
        START_DATE,
        END_DATE,
        LAG(END_DATE)
        OVER(
            ORDER BY
                START_DATE
        )
    FROM
        PROJECTS_P10
)
SELECT
    S_D,
    E_D,
    SUM(
        CASE
            WHEN PREV_END IS NULL THEN
                1
            WHEN PREV_END = S_D THEN
                0
            ELSE
                1
        END
    ) over (order by s_d)
FROM
    SORTED;
