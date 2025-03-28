/*
02. Changes in Net Worth

Writing a query to return the change in net worth for each user, 
ordered by decreasing net change. */


/*Querying original data*/
SELECT
    *
FROM
    ITEMS_P3;


/*Frequencies for each item by each date*/
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


/*rankings for each item by each date*/
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
