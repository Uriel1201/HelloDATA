/* 
01. Cancellation Rates.

From the following table of user IDs,
actions, and dates, write a query to
return the publication and cancellation
rate for each user. */

/* ORACLE. */
/********************************************************************/
CREATE TABLE USERS_01 (
    USER_ID INTEGER,
    ACTION  VARCHAR(9),
    DATES   DATE
);

INSERT INTO USERS_01
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
            3,
            'start',
            '07-jan-20'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'cancel',
            '08-jan-20'
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

/* SQLITE. */
/********************************************************************/
CREATE TABLE 
    USERS (
        USER_ID INTEGER,
        ACTION VARCHAR(9),
        DATES VARCHAR(9)
    );

"""
let users = vec![
    user!(1,
          "start",
          "01-jan-20"),
    user!(1,
          "cancel",
          "02-jan-20"),
    user!(2,
          "start",
          "03-jan-20"),
    user!(2,
          "publish",
          "04-jan-20"),
    user!(3,
          "start",
          "05-jan-20"),
    user!(3,
          "cancel",
          "06-jan-20"),
    user!(1,
          "start",
          "07-jan-20"),
    user!(1,
          "publish",
          "08-jan-20"),
    user!(0,
          "publish",
          "'08-jan-20"),
    ];

INSERT INTO 
    USERS 
    (USER_ID, ACTION, DATES)
VALUES
    (?1, ?2, ?3);
"""
