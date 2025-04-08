/*
03. Most Frequent Item

Writing a query to return the most frequent item
ordered on each date. */


/*Querying original data*/
SELECT
    *
FROM
    ITEMS_P3;


/*Frequencies for items on each date*/
CREATE TABLE FREQUENCIES_P3
    AS
        SELECT
            DATES,
            ITEM,
            COUNT(*) AS FREQUENCY
        FROM
            ITEMS_P3
        GROUP BY
            DATES,
            ITEM;


/*Querying frequencies*/
SELECT
    *
FROM
    FREQUENCIES_P3;


/*ranking items on each date*/
CREATE TABLE ITEMS_RANKING_P3
    AS
        SELECT
            DATES,
            ITEM,
            RANK()
            OVER(PARTITION BY DATES
                 ORDER BY
                     FREQUENCY DESC
            ) AS RANKING
        FROM
            FREQUENCIES_P3;


/*Querying rankings*/
SELECT
    *
FROM
    ITEMS_RANKING_P3;


/*Querying most frequent items on each date*/
SELECT
    DATES,
    ITEM
FROM
    ITEMS_RANKING_P3
WHERE
    RANKING = 1;



/*
with 
FREQUENCIES(dates, item, frequency)
    AS (
        SELECT
            DATES,
            ITEM,
            COUNT(*) AS FREQUENCY
        FROM
            ITEMS_P3
        GROUP BY
            DATES,
            ITEM), 
ITEMS_RANKING(dates, item, ranking)
    AS (
        SELECT
            DATES,
            ITEM,
            RANK()
            OVER(PARTITION BY DATES
                 ORDER BY
                     FREQUENCY DESC
            )
        FROM
            FREQUENCIES_P3)
SELECT
    DATES,
    ITEM
FROM
    ITEMS_RANKING_P3
WHERE
    RANKING = 1;
*/
