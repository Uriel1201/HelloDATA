with RANKINGS (
   id,
   action_date,
   ranked_dates
) AS (
        SELECT
            ID,
            ACTION_DATE,
            ROW_NUMBER()
            OVER(PARTITION BY ID
                 ORDER BY
                     ACTION_DATE DESC
            )
        FROM
            USERS_P4
  ), RANKED1 (
  id,
  first
  )
    AS (
        SELECT
            ID,
            ACTION_DATE
        FROM
            RANKINGS
        WHERE
            RANKED_DATES = 1
   ), RANKED2 (id, second)
    AS (
        SELECT
            ID,
            ACTION_DATE
        FROM
            RANKINGS
        WHERE
            RANKED_DATES = 2
   ) SELECT
    A.ID,
    ( A.first - B.second ) AS ELAPSED_TIME
FROM
    RANKED1 A
    LEFT JOIN RANKED2 B ON A.ID = B.ID
ORDER BY
    A.ID;
