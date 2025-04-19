select s.student_id, a.attendance
from students s
left join attendance a
on s.student_id=a.student_id
and extract(month from a.school_date)=extract(month from s.date_of_birth)
and extract(day from a.school_date)=extract(day from s.date_of_birth);
