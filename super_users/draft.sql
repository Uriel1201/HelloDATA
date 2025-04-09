with RANKINGS (
  user_id,
  transaction_date
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
      transaction_date
  ) AS (
        SELECT
            USER_ID,
            TRANSACTION_DATE
        FROM
            RANKINGS_P5
        WHERE
            RANKED_DATE = 2
  )
SELECT
    D.USER_ID,
    S.TRANSACTION_DATE
FROM
    DIUSERS_P5    D
    LEFT JOIN SUPERUSERS_P5 S ON D.USER_ID = S.USER_ID;
