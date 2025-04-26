/* 
01. Cancellation Rates.

Writing a query to return the publication and cancellation 
rate for each user. */

CREATE TABLE USERS_P1 (
    USER_ID INTEGER,
    ACTION  VARCHAR(9),
    DATES   DATE
);

INSERT INTO USERS_P1
    WITH NAMES AS (
        SELECT
            1,
            'start',
            '01-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'cancel',
            '02-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'start',
            '03-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'publish',
            '04-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'start',
            '05-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'cancel',
            '06-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'start',
            '07-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'publish',
            '08-jan-20'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;