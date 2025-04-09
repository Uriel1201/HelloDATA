/*
03. Most Frequent Item

Writing a query to return the most frequent item
ordered on each date. */

/*Querying original data*/
SELECT
    *
FROM
    ITEMS_P3;

/*Querying most frequent items on each date*/
WITH FREQUENCIES( --Frequencies for items on each date
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
), ITEMS_RANKING( --ranking items on each date
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
