import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import arrowkit

def main(table:str):

    conn = dbapi.connect("file:/content/my_SQLite.db?mode=ro")
    name = table.upper()
    if (name == "USERS_01"):

        try:

            arrow_users = arrowkit.get_ArrowTable(conn, table)
            print(f'RETURNING TABLE USERS_01 TABLE FROM DATABASE:\n{arrow_users.schema}')
            users = pol.from_arrow(arrow_users)
            print(":" * 40)
            print(f'USERS TABLE (Polars) -> SAMPLE\n{users.head(5)}')
            rates = (users.to_dummies(columns = 'ACTION')
                          .drop('DATES')
                          .group_by('USER_ID')
                          .agg(pol.col('*').sum())
                          .with_columns(publish_rate = pol.col('ACTION_publish') / pol.col('ACTION_start'),
                                        cancel_rate = pol.col('ACTION_cancel') / pol.col('ACTION_start')
                           )
            )

            print(":" * 40)
            print(f'USER STATISTICS (Polars) \n{rates}')

            query = """
                    WITH
                        TOTALS AS (
                            SELECT
                                USER_ID,
                                1.0 * SUM(CASE WHEN ACTION = "start" THEN 1 ELSE 0 END) AS TOTAL_STARTS,
                                1.0 * SUM(CASE WHEN ACTION = "cancel" THEN 1 ELSE 0 END) AS TOTAL_CANCELS,
                                1.0 * SUM(CASE WHEN ACTION = "publish" THEN 1 ELSE 0 END) AS TOTAL_PUBLISHES
                            FROM
                                USERS_01
                            GROUP BY
                                USER_ID)
            SELECT
                USER_ID,
                TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS, 0) AS PUBLISH_RATE,
                TOTAL_CANCELS / NULLIF(TOTAL_STARTS, 0) AS CANCEL_RATE
            FROM
                TOTALS
            ORDER BY
                1
            """
            result = arrowkit.get_ArrowTable(conn, query)
            df = result.to_pandas()
            print(":" * 40)
            print(f'USER STATISTICS (DuckDB)\n{result}')
            print(f'<*pandas visualization*>\n{df}')

        finally:

            conn.close()

    else:
  
        print(f'table {table} not available')
