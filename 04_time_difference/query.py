import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import arrowkit
import pandas as pd
import duckdb

def main(table:str):

    conn = dbapi.connect("file:/content/my_SQLite.db?mode=ro")
    name = table.upper()
    if (name == "USERS_04"):

        try:

            duck = duckdb.connect(":memory:")
            arrow_users = arrowkit.get_ArrowTable(conn, table)
            users = pol.from_arrow(arrow_users).lazy()

            sample = users.head(5)
            print(":" * 40)
            print(f'USERS TABLE (POLARS) -> SAMPLE:\n{sample.collect()}')
            users = (users.with_columns(pol.col('ACTION_DATE')
                                           .str
                                           .strptime(pol.Date,
                                                     format = "%d-%b-%y"
                                            )
                           )
            )
            result = (users.sort(by = ['ID','ACTION_DATE'],
                                 descending = [False,True]
                            )
                           .with_columns(ELAPSED_TIME = pol.col('ACTION_DATE')
                                                           .diff(-1)
                                                           .dt
                                                           .total_days()
                                                           .over(partition_by = 'ID')
                            )
                           .group_by('ID')
                           .first()
                           .select(pol.col("ID"), 
                                   pol.col("ELAPSED_TIME")
                            )
            )
            print(f'ELAPSED TIME BETWEEN TWO LAST ACTIONS MADE BY EACH USER\n(POLARS):\n{result.collect()}')
          
            query = """
                    WITH
                        DUCK_FORMATTED AS (
                            SELECT
                                ID,
                                ACTIONS,
                                DATE(STRPTIME(ACTION_DATE, '%d-%b-%y')) AS ACTION_DATE
                            FROM
                                'arrow_users'),
                        ORDERED_DATES AS (
                            SELECT
                                ID,
                                ACTION_DATE,
                                ROW_NUMBER() OVER (PARTITION BY
                                                       ID
                                                   ORDER BY
                                                       ACTION_DATE
                                                   DESC) AS ORDERED
                            FROM
                                DUCK_FORMATTED),
                        LAST_DATES AS (
                            SELECT
                                ID,
                                ACTION_DATE AS LAST_DATE
                            FROM
                                ORDERED_DATES
                            WHERE
                                ORDERED = 1),
                        PENULTIMATE_DATES AS (
                            SELECT
                                ID,
                                ACTION_DATE AS PENULTIMATE_DATE
                            FROM
                                ORDERED_DATES
                            WHERE
                                ORDERED = 2)
            SELECT
                L.ID,
                (L.LAST_DATE - P.PENULTIMATE_DATE) AS ELAPSED_TIME
            FROM
                LAST_DATES L
                LEFT JOIN
                    PENULTIMATE_DATES P
                USING (ID)
            ORDER BY
                1
            """
            duck_result = duck.sql(query).fetch_arrow_table()
            df = duck_result.to_pandas()
            print(":" * 40)
            print(f'ELAPSED TIME BETWEEN TWO LAST ACTIONS MADE BY EACH USER\n(DUCKDB QUERIES):\n{duck_result}')
            print(f'<*pandas visualization*>\n{df}')

        finally:

            conn.close()
            duck.close()

    else:

        print(f'TABLE {table} NOT AVAILABLE')
