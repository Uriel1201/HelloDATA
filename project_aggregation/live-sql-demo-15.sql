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

/* Returning the duration of each project*/
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
), flags( -- identifying unique projects 
    s_d,
    e_d,
    project_id
) as (
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
    SORTED)
select min(s_d) as project_start, max(e_d) as project_end, max(e_d)-min(s_d) as duration 
from flags
group by project_id
order by 3,1;

