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
    _START,
    _END,
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
    _START,
    _END,
    PROJECT_ID
) AS (
    SELECT
        _START,
        _END,
        SUM(
            CASE
                WHEN _END IS NULL THEN
                    1
                WHEN _END = _START THEN
                    0
                ELSE 1
            END
        )
        OVER(
            ORDER BY
                _START
        ) AS PROJECT_ID
    FROM
        SORTED
)
SELECT
    MIN(_START)             AS PROJECT_START,
    MAX(_END)               AS PROJECT_END,
    MAX(_END) - MIN(_START) AS DURATION
FROM
    PROJECT_FLAGS
GROUP BY
    PROJECT_ID
ORDER BY
    3,
    1;