/*
Writing a query to get, for each month, the
cumulative sum of an employee’s salary
over a period of 3 months, excluding the
most recent month. */

WITH RANKINGS (
    ID,
    MONTH,
    CUMMULATIVE,
    RANKING
) AS (
    SELECT
        ID,
        PAY_MONTH,
        SUM(SALARY)
        OVER(PARTITION BY ID
             ORDER BY
                 PAY_MONTH
        ),
        RANK()
        OVER(PARTITION BY ID
             ORDER BY
                 PAY_MONTH DESC
        )
    FROM
        EMPLOYEE_P14
)
SELECT
    ID,
    CUMMULATIVE
FROM
    RANKINGS
WHERE
        RANKING > 1
    AND RANKING <= 4
ORDER BY
    1,
    2;
