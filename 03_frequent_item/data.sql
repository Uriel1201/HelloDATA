/*
03. Most Frequent Item

From the following table containing a list
of dates and items ordered, write a
query to return the most frequent item
ordered on each date. Return multiple
items in the case of a tie. */

/* ORACLE. */

/********************************************************************/
CREATE TABLE ITEMS_P3 (
    DATES DATE,
    ITEM  VARCHAR(9)
);

INSERT INTO ITEMS_P3
    WITH NAMES AS (
        SELECT
            '01-jan-20',
            'apple'
        FROM
            DUAL
        UNION ALL
        SELECT
            '01-jan-20',
            'apple'
        FROM
            DUAL
        UNION ALL
        SELECT
            '01-jan-20',
            'pear'
        FROM
            DUAL
        UNION ALL
        SELECT
            '01-jan-20',
            'pear'
        FROM
            DUAL
        UNION ALL
        SELECT
            '02-jan-20',
            'pear'
        FROM
            DUAL
        UNION ALL
        SELECT
            '02-jan-20',
            'pear'
        FROM
            DUAL
        UNION ALL
        SELECT
            '02-jan-20',
            'pear'
        FROM
            DUAL
        UNION ALL
        SELECT
            '02-jan-20',
            'orange'
        FROM
            DUAL
    )
    SELECT
        *
    FROM
        NAMES;

/* DUCKDB. */

/********************************************************************/
CREATE TABLE 
    ITEMS (
           DATES DATE,
           ITEM  VARCHAR
    );

INSERT INTO 
    ITEMS
BY POSITION VALUES 
    ("2020-01-01", "apple"),
    ("2020-01-01", "apple"),
    ("2020-01-01", "pear"),
    ("2020-01-01", "pear"),
    ("2020-01-02", "pear"),
    ("2020-01-02", "pear"),
    ("2020-01-02", "pear"),
    ("2020-01-02", "orange");
