
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

with sorted(_start,
            _end,
            prev_end) as
(select start_date, end_date, lag(end_date) over (order by start_date)
  from projects_p10),
project_flags(_start, _end, project_id) as (
  select _start, _end, sum(case when _end is null then 1,
                           else if _end=_start then 0
                           else 1 end) over (order by _start) as project_id
  from sorted
)select min(_start) as project_start,
        max(_end) as project_end,
       max(_end) -min(_start) as duration 
from project_flags 
group by project_id
order by 3,1;
