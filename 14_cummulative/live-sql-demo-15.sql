/*
Writing a query to get, for each month, the
cumulative sum of an employee’s salary
over a period of 3 months, excluding the
most recent month. */

CREATE TABLE EMPLOYEE_P14 (
    ID        INTEGER,
    PAY_MONTH INTEGER,
    SALARY    INTEGER
);

INSERT INTO EMPLOYEE_P14
    WITH NAMES AS (
        SELECT
            1 I,
            1,
            20
        FROM
            DUAL
        UNION ALL
        SELECT
            2 I,
            1,
            20
        FROM
            DUAL
        UNION ALL
        SELECT
            1 I,
            2,
            30
        FROM
            DUAL
        UNION ALL
        SELECT
            2 I,
            2,
            30
        FROM
            DUAL
        UNION ALL
        SELECT
            3 I,
            2,
            40
        FROM
            DUAL
        UNION ALL
        SELECT
            1 I,
            3,
            40
        FROM
            DUAL
        UNION ALL
        SELECT
            3 I,
            3,
            60
        FROM
            DUAL
        UNION ALL
        SELECT
            1 I,
            4,
            60
        FROM
            DUAL
        UNION ALL
        SELECT
            3 I,
            4,
            70
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;