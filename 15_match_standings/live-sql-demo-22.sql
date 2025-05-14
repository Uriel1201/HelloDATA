/*
15. Team Standings.

Writing a query to return the scores of
each team */

CREATE TABLE TEAMS_P15 (
    TEAM_ID   INTEGER,
    TEAM_NAME VARCHAR(20)
);

CREATE TABLE MATCHES_P15 (
    MATCH_ID    INTEGER,
    HOST_TEAM   INTEGER,
    GUEST_TEAM  INTEGER,
    HOST_GOALS  INTEGER,
    GUEST_GOALS INTEGER
);

INSERT INTO TEAMS_P15
    WITH NAMES AS (
        SELECT
            1,
            'New York'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'Atlanta'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'Chicago'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            'Toronto'
        FROM
            DUAL
        UNION ALL
        SELECT
            5,
            'Los Angeles'
        FROM
            DUAL
        UNION ALL
        SELECT
            6,
            'Seattle'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

INSERT INTO MATCHES_P15
    WITH NAMES AS (
        SELECT
            1 M,
            1,
            2,
            3,
            0
        FROM
            DUAL
        UNION ALL
        SELECT
            2 M,
            2,
            3,
            2,
            4
        FROM
            DUAL
        UNION ALL
        SELECT
            3 M,
            3,
            4,
            4,
            3
        FROM
            DUAL
        UNION ALL
        SELECT
            4 M,
            4,
            5,
            1,
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            5 M,
            5,
            6,
            2,
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            6 M,
            6,
            1,
            1,
            2
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;