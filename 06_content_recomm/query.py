import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import arrowkit
import pandas as pd
import duckdb

def main(table1:str, table2:str):

    conn = dbapi.connect("file:/content/my_SQLite.db?mode=ro")
    name1 = table1.upper()
    name2 = table2.upper()
    if (name1 == "FRIENDS_06") and (name2 == "LIKES_06"):

        try:

            duck = duckdb.connect(":memory:")
            arrow_friends = arrowkit.get_ArrowTable(conn, table1)
            arrow_likes = arrowkit.get_ArrowTable(conn, table2)

            friends = pol.from_arrow(arrow_friends).lazy()
            likes = pol.from_arrow(arrow_likes).lazy()

            sample1 = friends.head(5)
            sample2 = likes.head(5)
            print(":" * 40)
            print(f'FRIENDS TABLE (POLARS) -> SAMPLE:\n{sample1.collect()}')
            print(f'LIKES TABLE (POLARS) -> SAMPLE:\n{sample2.collect()}')

            result = (friends.join(likes,
                                   left_on = "FRIEND",
                                   right_on = "USER_ID",
                                   coalesce = True
                              )
                             .select(pol.col("USER_ID"), 
                                     pol.col("PAGE_LIKES")
                                        .alias("RECOMMENDATION")
                              )
                             .join(likes, 
                                   left_on = ["USER_ID", "RECOMMENDATION"], 
                                   right_on = ["USER_ID", "PAGE_LIKES"], 
                                   coalesce = True, 
                                   how = "anti"
                              )
                             .unique()
            )
            print(":" * 40)
            print(f'RETURNING RECOMMENDATIONS FOR EACH USER (POLARS):\n{result.collect()}')

            query = """
                    WITH
                        RECOMMENDATIONS AS (
                            SELECT
                                F.USER_ID,
                                L.PAGE_LIKES AS RECOMMENDATION
                            FROM
                                'arrow_friends' F
                                INNER JOIN 'arrow_likes' L
                                    ON F.FRIEND = L.USER_ID)
            SELECT DISTINCT
                R.USER_ID,
                R.RECOMMENDATION
            FROM
                RECOMMENDATIONS R
                ANTI JOIN 'arrow_likes' L
                    ON
                        R.USER_ID = L.USER_ID
                    AND
                        R.RECOMMENDATION = L.PAGE_LIKES
            ORDER BY
                1, 2
            """
            duck_result = duck.sql(query).fetch_arrow_table()
            df = duck_result.to_pandas()
            print(":" * 40)
            print(f'RETURNING RECOMMENDATIONS FOR EACH USER (DUCKDB QUERIES):\n{duck_result}')
            print(f'<*pandas visualization*>\n{df}')

        finally:

            conn.close()
            duck.close()

    else:

        print(f'TABLE {table1} OR TABLE {table2} NOT AVAILABLE')
