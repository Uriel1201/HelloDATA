/*
03. Most Frequent Item

Writing a query to return the most frequent item
ordered on each date. */

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
