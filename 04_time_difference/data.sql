/* 
04. Time Difference Between Latest Actions

Writing a query to return for each user the
time elapsed between the last action
and the second-to-last action, in
ascending order by user ID. */

/* ORACLE. */

/********************************************************************/
CREATE TABLE USERS_P4 (
    ID          INTEGER,
    ACTIONS     VARCHAR(10),
    ACTION_DATE DATE
);

INSERT INTO USERS_P4
    WITH NAMES AS (
        SELECT
            1,
            'Start',
            '13-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'Cancel',
            '13-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'Start',
            '11-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'Publish',
            '14-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'Start',
            '15-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'Cancel',
            '15-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            'Start',
            '18-feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'Publish',
            '19-feb-20'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

/* DUCKDB. */

/********************************************************************/
CREATE TABLE 
    USERS (
           ID          INTEGER,
           ACTIONS     VARCHAR,
           ACTION_DATE DATE
    );

INSERT INTO
    USERS
BY POSITION VALUES 
    (1,
     "Start",
     "2020-02-13"),
    (1,
     "Cancel",
     "2020-02-13"),
    (2,
     "Start",
     "2020-02-11"),
    (2,
     "Publish",
     "2020-02-14"),
    (3,
     "Start",
     "2020-02-15"),
    (3,
     "Cancel",
     "2020-02-15"),
    (4,
     "Start",
     "2020-02-18"),
    (1,
     "Publish",
     "2020-02-19");
