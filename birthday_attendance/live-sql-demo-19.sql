with birthday_attendance(
    student_id,
    attendance
) as (
    SELECT
    S.STUDENT_ID,
    A.ATTENDANCE
FROM
    STUDENTS_P11   S
    LEFT JOIN ATTENDANCE_P11 A ON S.STUDENT_ID = A.STUDENT_ID
                                  AND EXTRACT(MONTH FROM A.SCHOOL_DATE) = EXTRACT(MONTH FROM S.DATE_BIRTH)
                                  AND EXTRACT(DAY FROM A.SCHOOL_DATE) = EXTRACT(DAY FROM S.DATE_BIRTH)
    ) 
select round(avg(attendance),2)
from birthday_attendance;
