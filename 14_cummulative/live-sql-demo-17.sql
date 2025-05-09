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
    RANKING > 1;