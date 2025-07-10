/*
03. Most Frequent Item

From the following table containing a list
of dates and items ordered, write a
query to return the most frequent item
ordered on each date. Return multiple
items in the case of a tie. */

/* ORACLE. */

/********************************************************************/
-- Querying original data
SELECT
    *
FROM
    ITEMS_P3;

      -- Querying most frequent items on each date
WITH FREQUENCIES ( --Frequencies for items on each date
    DATES,
    ITEM,
    FREQUENCY
) AS (
    SELECT
        DATES,
        ITEM,
        COUNT(*)
    FROM
        ITEMS_P3
    GROUP BY
        DATES,
        ITEM
), ITEMS_RANKING ( --ranking items on each date
    DATES,
    ITEM,
    RANKING
) AS (
    SELECT
        DATES,
        ITEM,
        RANK()
        OVER(PARTITION BY DATES
             ORDER BY
                 FREQUENCY DESC
        )
    FROM
        FREQUENCIES
)
SELECT
    DATES,
    ITEM
FROM
    ITEMS_RANKING
WHERE
    RANKING = 1;

/* DUCKDB. */

/********************************************************************/
SELECT 
    *
FROM
    'arrow_items'
USING SAMPLE 50%
    (bernoulli);

WITH 
    FREQUENCIES AS (
        SELECT
            DATES, 
            ITEM, 
            COUNT(*) AS FREQUENCY 
        FROM 
            'arrow_items'
        GROUP BY 
            DATES, 
            ITEM),
    RANKS AS (
        SELECT
            DATES, 
            ITEM, 
            RANK() OVER (PARTITION BY 
                             DATES 
                         ORDER BY 
                             FREQUENCY DESC) AS RANKED 
        FROM 
            FREQUENCIES) 
SELECT 
    DATES, 
    ITEM
FROM 
    RANKS
WHERE 
    RANKED = 1
ORDER BY
    1;
