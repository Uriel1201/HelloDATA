select start_date
from projects_p10
where start_date not in 
(select end_date from
  projects_p10);
