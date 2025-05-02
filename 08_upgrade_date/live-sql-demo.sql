/* 
08. Upgrade Rate by Product Action

Returning the fraction of users, rounded to two
decimal places, who first accessed feature
two (type: F2 in events table) and
upgraded to premium within the first 30
days of signing up. */

CREATE TABLE USERS_P8 (
    USER_ID   INTEGER,
    NAME      VARCHAR(9),
    JOIN_DATE DATE
);

CREATE TABLE EVENTS_P8 (
    USER_ID     INTEGER,
    TYPE        VARCHAR(2),
    ACCESS_DATE DATE
);

INSERT INTO USERS_P8
    WITH NAMES AS (
        SELECT
            1,
            'John',
            '14-Feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'Jane',
            '14-Feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'Jill',
            '15-Feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            'Josh',
            '15-Feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            5,
            'Jean',
            '16-Feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            6,
            'Justin',
            '17-Feb-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            7,
            'Jeremy',
            '18-Feb-20'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

INSERT INTO EVENTS_P8
    WITH NAMES AS (
        SELECT
            1,
            'F1',
            '1-Mar-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'F2',
            '2-Mar-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'P',
            '12-Mar-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'F2',
            '15-Mar-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            'F2',
            '15-Mar-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'P',
            '16-Mar-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'P',
            '22-Mar-20'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;