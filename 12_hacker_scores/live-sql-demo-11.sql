/* 
12. Hacker Scores.

Writing  a
query to return the hacker ID, name, and
total score (the sum of maximum scores
for each challenge completed) ordered by descending score, and by ascending
hacker ID in the case of score tie. Do not
display entries for hackers with a score
of zero. */

CREATE TABLE HACKERS_P12 (
    HACKER_ID INTEGER,
    NAME      VARCHAR(10)
);

INSERT INTO HACKERS_P12
    WITH NAMES AS (
        SELECT
            1,
            'John'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'Jane'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'Joe'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            'Jim'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

CREATE TABLE SUBMISSIONS_P12 (
    SUB_ID       INTEGER,
    HACKER_ID    INTEGER,
    CHALLENGE_ID INTEGER,
    SCORE        INTEGER
);

INSERT INTO SUBMISSIONS_P12
    WITH NAMES AS (
        SELECT
            101 S,
            1   H,
            1   CH,
            10  SC
        FROM
            DUAL
        UNION ALL
        SELECT
            102 S,
            1   H,
            1   CH,
            12  SC
        FROM
            DUAL
        UNION ALL
        SELECT
            103 S,
            2   H,
            1   CH,
            11  SC
        FROM
            DUAL
        UNION ALL
        SELECT
            104 S,
            2   H,
            1   CH,
            9   SC
        FROM
            DUAL
        UNION ALL
        SELECT
            105 S,
            2   H,
            2   CH,
            13  SC
        FROM
            DUAL
        UNION ALL
        SELECT
            106 S,
            3   H,
            1   CH,
            9   SC
        FROM
            DUAL
        UNION ALL
        SELECT
            107 S,
            3   H,
            2   CH,
            12  SC
        FROM
            DUAL
        UNION ALL
        SELECT
            108 S,
            3   H,
            2   CH,
            15  SC
        FROM
            DUAL
        UNION ALL
        SELECT
            109 S,
            4   H,
            1   CH,
            0   SC
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;