/* 
01. Cancellation Rates.

From the following table of user IDs,
actions, and dates, write a query to
return the publication and cancellation
rate for each user. */

/* ORACLE. */

/********************************************************************/

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

/* DUCKDB. */

/********************************************************************/

CREATE TABLE USERS(USER_ID INTEGER,
                   ACTION VARCHAR,
                   DATES DATE,
                   PRIMARY KEY(USER_ID, DATES)
                  );

INSERT INTO USERS BY POSITION VALUES (1, "start", "2020-01-01"),
                                     (1, "cancel", "2020-01-02"),
                                     (2, "start", "2020-01-03"),
                                     (2, "publish", "2020-01-04"),
                                     (3, "start", "2020-01-05"),
                                     (3, "cancel", "2020-01-06" ),
                                     (1, "start", "2020-01-07"),
                                     (1, "publish", "2020-01-08");
