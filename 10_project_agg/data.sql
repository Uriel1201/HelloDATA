/* 
10. Project Aggregation 

Writing a query to return the start and end
dates of each project, and the number of
days it took to complete. */

CREATE TABLE PROJECTS_P10 (
    TASK_ID    INTEGER,
    START_DATE DATE,
    END_DATE   DATE
);

INSERT INTO PROJECTS_P10
    WITH NAMES AS (
        SELECT
            1,
            '01-Oct-20',
            '02-Oct-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            '02-Oct-20',
            '03-Oct-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            '03-Oct-20',
            '04-Oct-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            '13-Oct-20',
            '14-Oct-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            5,
            '14-Oct-20',
            '15-Oct-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            6,
            '28-Oct-20',
            '29-Oct-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            7,
            '30-Oct-20',
            '31-Oct-20'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;
