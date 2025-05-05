/* 
11. Birthday Attendance.

Writing a query to return the fraction of students,
rounded to two decimal places, who
attended school on their birthday. */

CREATE TABLE ATTENDANCE_P11 (
    STUDENT_ID  INTEGER,
    SCHOOL_DATE DATE,
    ATTENDANCE  INTEGER
);

CREATE TABLE STUDENTS_P11 (
    STUDENT_ID INTEGER,
    SCHOOL_ID  INTEGER,
    GRADE      INTEGER,
    DATE_BIRTH DATE
);

INSERT INTO STUDENTS_P11
    WITH NAMES AS (
        SELECT
            1,
            2,
            5,
            '3-Apr-12'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            1,
            4,
            '4-Apr-13'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            1,
            3,
            '5-Apr-14'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            2,
            4,
            '3-Apr-13'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

INSERT INTO ATTENDANCE_P11
    WITH NAMES AS (
        SELECT
            1,
            '3-Apr-20',
            0
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            '3-Apr-20',
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            '3-Apr-20',
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            '4-Apr-20',
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            '4-Apr-20',
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            '4-Apr-20',
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            '5-Apr-20',
            0
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            '5-Apr-20',
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            '5-Apr-20',
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            '5-Apr-20',
            1
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;