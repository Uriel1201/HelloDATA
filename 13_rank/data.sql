/*
13. Rank Without Rank.

Writing a query to rank scores in the
following table without using a window
function. */

CREATE TABLE SCORES_P13 (
    ID    INTEGER,
    SCORE NUMBER
);

INSERT INTO SCORES_P13
    WITH NAMES AS (
        SELECT
            1,
            3.50
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            3.65
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            4.00
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            3.85
        FROM
            DUAL
        UNION ALL
        SELECT
            5,
            4.00
        FROM
            DUAL
        UNION ALL
        SELECT
            6,
            3.65
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;
