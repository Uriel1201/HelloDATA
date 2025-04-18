/* 
10. Project Aggregation 

Writing a query to return the start and end
dates of each project, and the number of
days it took to complete. */

/* Querying original data*/
SELECT
    *
FROM
    PROJECTS_P10;

/*Querying the duration of each unique project.*/
WITH SORTED ( -- ordering dates
    S_d,
    E_d,
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
), PROJECT_FLAGS ( --- identifying unique projects 
    S_d,
    E_d,
    PROJECT_ID
) AS (
    SELECT
        S_d,
        e_d,
        SUM(
            CASE
                WHEN e_d IS NULL THEN
                    1
                WHEN e_d = s_d THEN
                    0
                ELSE 1
            END
        )
        OVER(
            ORDER BY
                s_d
        ) AS PROJECT_ID
    FROM
        SORTED
)
SELECT
    MIN(s_d)             AS PROJECT_START,
    MAX(e_d)               AS PROJECT_END,
    MAX(e_d) - MIN(s_d) AS DURATION
FROM
    PROJECT_FLAGS
GROUP BY
    PROJECT_ID
ORDER BY
    3,
    1;
