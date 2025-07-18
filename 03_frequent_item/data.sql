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

/* SQLITE/

/********************************************************************/
"CREATE TABLE 
     ITEMS(
         DATE VARCHAR(9),
         ITEM VARCHAR(9)
     )"
"""
    let items = vec![
    item!("apple", "01-jan-20"),
    item!("apple", "01-jan-20"),
    item!("pear", "01-jan-20"),
    item!("pear", "01-jan-20"),
    item!("pear", "02-jan-20"),
    item!("pear", "02-jan-20"),
    item!("pear", "02-jan-20"),
    item!("orange", "02-jan-20"),
    item!("Dewey", "fuaaaa!!!"),
    item!("Thanos", "67-jan-93"),
    ];
"""
"INSERT INTO ITEMS(DATE, ITEM) 
     values (?1, ?2)"
