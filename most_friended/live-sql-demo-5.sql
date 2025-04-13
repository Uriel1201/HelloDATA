/* 
09. Most Friended.

Returning a list of
users and their corresponding friend
count. Assuming that only
unique friendships are displayed. */

CREATE TABLE FRIENDS_P9 (
    USER_1 INTEGER,
    USER_2 INTEGER
);

INSERT INTO FRIENDS_P9
    WITH NAMES AS (
        SELECT
            1,
            2
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            3
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            4
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            3
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;