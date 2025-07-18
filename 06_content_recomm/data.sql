/*  
06. Content Recommendations

Using the following two tables, write a
query to return page recommendations
to a social media user based on the
pages that their friends have liked, but
that they have not yet marked as liked.
Order the result by ascending user ID. */

/* ORACLE. */

/********************************************************************/
CREATE TABLE FRIENDS_P6 (
    USER_ID INTEGER,
    FRIEND  INTEGER
);

CREATE TABLE LIKES_P6 (
    USER_ID    INTEGER,
    PAGE_LIKES CHAR
);

INSERT INTO FRIENDS_P6
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
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
            4
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            1
        FROM
            DUAL
        UNION ALL
        SELECT
            4,
            3
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

INSERT INTO LIKES_P6
    WITH NAMES AS (
        SELECT
            1,
            'A'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'B'
        FROM
            DUAL
        UNION ALL
        SELECT
            1,
            'C'
        FROM
            DUAL
        UNION ALL
        SELECT
            2,
            'A'
        FROM
            DUAL
        UNION ALL
        SELECT
            3,
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
CREATE TABLE 
    FRIENDS (
        USER_ID INTEGER,
        FRIEND INTEGER
    );

CREATE TABLE 
    LIKES (
        USER_ID INTEGER,
        PAGE_LIKES CHAR
    );

"""
let friends = vec![
    friend!(1,
            2),
    friend!(1,
            3),
    friend!(1,
            4),
    friend!(2,
            1),
    friend!(3,
            1),
    friend!(3,
            4),
    friend!(4,
            1),
    friend!(4,
            3),
];
"""
"""
let likes = vec![
    like!(1,
          "A"),
    like!(1,
          "B"),
    like!(1,
          "C"),
    like!(2,
          "A"),
    like!(3,
          "B"),
    like!(3,
          "C"),
    like!(4,
          "B"),
];
"""
INSERT INTO 
    FRIENDS 
    (USER_ID, FRIEND)
VALUES
    (?1, ?2);

INSERT INTO 
    LIKES 
    (USER_ID, PAGE_LIKES)
VALUES
    (?1, ?2);
