/* 
04. Time Difference Between Latest Actions

writing a query to return for each user the
time elapsed between the last action
and the second-to-last action, in
ascending order by user ID. */ 


SELECT
    *
FROM
    USERS_P4;


CREATE TABLE RANKINGS_P4
    AS
        SELECT
            ID,
            ACTION_DATE,
            ROW_NUMBER()
            OVER(PARTITION BY ID
                 ORDER BY
                     ACTION_DATE DESC
            ) AS RANKED_DATES
        FROM
            USERS_P4;


SELECT
    *
FROM
    RANKINGS_P4;


CREATE TABLE RANKED1_P4
    AS
        SELECT
            ID,
            ACTION_DATE AS RANKED_ONE
        FROM
            RANKINGS_P4
        WHERE
            RANKED_DATES = 1;


SELECT
    *
FROM
    RANKED1_P4;


CREATE TABLE RANKED2_P4
    AS
        SELECT
            ID,
            ACTION_DATE AS RANKED_TWO
        FROM
            RANKINGS_P4
        WHERE
            RANKED_DATES = 2;


SELECT
    *
FROM
    RANKED2_P4;
