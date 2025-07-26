/* 
07. Mobile and Web Visitors.

Returning the
fraction of users who only visited
mobile, only visited web, and visited
both. */

/* ORACLE. */

/********************************************************************/
CREATE TABLE MOBILE_P7 (
    USER_ID  INTEGER,
    PAGE_URL CHAR
);

CREATE TABLE WEB_P7 (
    USER_ID  INTEGER,
    PAGE_URL CHAR
);

INSERT INTO MOBILE_P7
    WITH NAMES AS (
        SELECT
            1,
            'A'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'B'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'C'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            'A'
        FROM
            DUAL
        UNION ALL
        SELECT
            9,
            'B'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'C'
        FROM
            DUAL
        UNION ALL
        SELECT
            10,
            'B'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

INSERT INTO WEB_P7
    WITH NAMES AS (
        SELECT
            6,
            'A'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'B'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            'C'
        FROM
            DUAL
        UNION ALL
        SELECT
            7,
            'A'
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            'B'
        FROM
            DUAL
        UNION ALL
        SELECT
            8,
            'C'
        FROM
            DUAL
        UNION ALL
        SELECT
            5,
            'B'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

/* RUSQLITE. */

/********************************************************************/
