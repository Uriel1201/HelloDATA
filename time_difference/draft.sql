with RANKINGS (
  id
)
    AS (
        SELECT
            ID,
            ACTION_DATE,
            ROW_NUMBER()
            OVER(PARTITION BY ID
                 ORDER BY
                     ACTION_DATE DESC
            ) AS RANKED_DATES
        FROM
            USERS_P4
  )


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


SELECT
    A.ID,
    ( A.RANKED_ONE - B.RANKED_TWO ) AS ELAPSED_TIME
FROM
    RANKED1_P4 A
    LEFT JOIN RANKED2_P4 B ON A.ID = B.ID
ORDER BY
    A.ID;
