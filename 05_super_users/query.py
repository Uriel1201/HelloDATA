import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import arrowkit
import pandas as pd
import duckdb

def main(table:str):

    conn = dbapi.connect("file:/content/my_SQLite.db?mode=ro")
    name = table.upper()
    if (name == "USERS_05"):

        try:

            duck = duckdb.connect(":memory:")
            arrow_users = arrowkit.get_ArrowTable(conn, table)
            users = pol.from_arrow(arrow_users).lazy()

            sample = users.head(5)
            print(":" * 40)
            print(f'USERS TABLE (POLARS) -> SAMPLE:\n{sample.collect()}')

            super = (users.select(pol.col("USER_ID"),
                                  pol.col("TRANSACTION_DATE")
                           )
                          .with_columns(pol.col("TRANSACTION_DATE")
                                           .str
                                           .strptime(pol.Date,
                                                     format = "%d-%b-%y"
                                            )
                           )
                          .sort(by = ["USER_ID", "TRANSACTION_DATE"])
                          .with_columns(pol.col("TRANSACTION_DATE")
                                           .rank(method = "ordinal")
                                           .over(partition_by = "USER_ID")
                                           .alias("TRANSACTION")
                           )
                          .filter(pol.col("TRANSACTION") == 2)
            )
            users = (users.select(pol.col("USER_ID"))
                          .unique()
            )
            result = (users.join(super,
                                 on = "USER_ID",
                                 how = "left"
                            )
            )
            print(":" * 40)
            print(f'USERS BECOMING SUPERUSERS (POLARS):\n{result.collect()}')

            query = """
                    WITH
                        DUCK_FORMATTED AS (
                            SELECT
                                USER_ID,
                                DATE(STRPTIME(TRANSACTION_DATE, '%d-%b-%y')) AS TRANSACTION_DATE
                            FROM
                                'arrow_users'),
                        RANKINGS AS (
                            SELECT
                                USER_ID,
                                TRANSACTION_DATE,
                                ROW_NUMBER() OVER(PARTITION BY
                                                      USER_ID
                                                  ORDER BY
                                                      TRANSACTION_DATE) AS RANKED_DATE
                            FROM
                                DUCK_FORMATTED),
                        USER_ AS (
                            SELECT DISTINCT
                                USER_ID
                            FROM
                                'arrow_users'),
                        SUPERUSERS AS (
                            SELECT
                                USER_ID,
                                TRANSACTION_DATE AS DATE_AS_SUPER
                            FROM
                                RANKINGS
                            WHERE
                                RANKED_DATE = 2)
            SELECT
                U.USER_ID,
                S.DATE_AS_SUPER
            FROM
                USER_      U
                LEFT JOIN
                    SUPERUSERS S
                USING (USER_ID)
            ORDER BY
                2
            """
            duck_result = duck.sql(query).fetch_arrow_table()
            df = duck_result.to_pandas()
            print(":" * 40)
            print(f'USERS BECOMING SUPERUSERS (DUCKDB QUERIES):\n{duck_result}')
            print(f'<*pandas visualization*>\n{df}')

        finally:

            conn.close()
            duck.close()

    else:

        print(f'TABLE {table} NOT AVAILABLE')
