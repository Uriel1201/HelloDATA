with RANKINGS (
  user_id,
  transaction_date,
  ranked_date
) AS (
        SELECT
            USER_ID,
            TRANSACTION_DATE,
            ROW_NUMBER()
            OVER(PARTITION BY USER_ID
                 ORDER BY
                     TRANSACTION_DATE
            )
        FROM
            USERS_P5
  ), USERS (
     user_id
  ) AS (
        SELECT DISTINCT
            ( USER_ID )
        FROM
            USERS_P5
  ), SUPERUSERS (
      user_id,
      date_as_super
  ) AS (
        SELECT
            USER_ID,
            TRANSACTION_DATE
        FROM
            RANKINGS
        WHERE
            RANKED_DATE = 2
  )
SELECT
    U.USER_ID,
    S.date_as_super
FROM
    USERS   U
    LEFT JOIN SUPERUSERS S ON U.USER_ID = S.USER_ID
order by 2;
