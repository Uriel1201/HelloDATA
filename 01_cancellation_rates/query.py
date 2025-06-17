#python -m pip install adbc_driver_sqlite duckdb --upgrade
import requests

arrow_kit = "https://github.com/Uriel1201/HelloDATA/raw/refs/heads/main/SQLiteArrowKit.py"
my_db = "https://github.com/Uriel1201/HelloDATA/raw/refs/heads/main/my_SQLite.db"
response1 = requests.get(arrow_kit)
response2 = requests.get(my_db)
with open('arrowkit.py', 'wb') as f:
    f.write(response1.content)

with open("my_SQLite.db", "wb") as f:
    f.write(response2.content)

import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import duckdb
import arrowkit

def main(table:str):

    conn = dbapi.connect("file:/content/my_SQLite.db?mode=ro")

    if arrowkit.is_available(conn, table) and (table == "users_01"):

        try:

            arrow_users = arrowkit.get_ArrowTable(conn, table)
            users = pol.from_arrow(arrow_users)

            print("\n" + ":" * 40)
            print(f'USERS TABLE USING POLARS:\n{users.head(5)}')
            rates = (users.to_dummies(columns = 'ACTION')
                          .drop('DATES')
                          .group_by('USER_ID')
                          .agg(pol.col('*').sum())
                          .with_columns(publish_rate = pol.col('ACTION_publish') / pol.col('ACTION_start'),
                                        cancel_rate = pol.col('ACTION_cancel') / pol.col('ACTION_start')
                           )
            )

            print("\n" + ":" * 40)
            print(f'USER STATISTICS, USING POLARS:\n{rates}')

            duck = duckdb.connect(":memory:")

            query = """
            WITH
                DUCK_UPDATED AS (
                    SELECT
                        USER_ID,
                        ACTION,
                        STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
                    FROM
                        arrow_users),
                TOTALS AS (
                    SELECT
                        USER_ID,
                        SUM(IF(ACTION = 'start',1,0)) AS TOTAL_STARTS,
                        SUM(IF(ACTION = 'cancel',1,0)) AS TOTAL_CANCELS,
                        SUM(IF(ACTION = 'publish',1,0)) AS TOTAL_PUBLISHES
                    FROM
                        DUCK_UPDATED
                    GROUP BY
                        USER_ID)
            SELECT
                USER_ID,
                ROUND(TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS,
                                               0),
                      2) AS PUBLISH_RATE,
                ROUND(TOTAL_CANCELS / NULLIF(TOTAL_STARTS,
                                             0),
                      2) AS CANCEL_RATE
            FROM
                TOTALS
            ORDER BY
                1
            """
            print("\n" + ":" * 40)
            print(f'USER STATISTICS, USING DUCKDB QUERIES:')
            duck.sql(query).show()

        finally:

            conn.close()
            duck.close()

    else:
  
        print(f'table {table} not available')
