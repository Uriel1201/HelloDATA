import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import arrowkit
import pandas as pd

def main(table:str):

    conn = dbapi.connect("file:/content/my_SQLite.db?mode=ro")

    if (table == "items_03"):

        try:
       
            arrow_items = arrowkit.get_ArrowTable(conn, table)
            items = pol.from_arrow(arrow_items).lazy()

            sample = items.head(5)
            print(":" * 40)
            print(f'ITEMS TABLE USING POLARS LAZYFRAMES -> SAMPLE:\n{sample.collect()}')
            
            pol_ranks = (items.group_by(["DATES", "ITEM"])
                              .agg(pol.len()
                                      .alias("FREQUENCY")
                               )
                              .with_columns(pol.max("FREQUENCY")
                                               .over(partition_by = "DATES")
                                               .alias("MAX_FREQUENCY")
                               )
                              .filter(pol.col("FREQUENCY") == pol.col("MAX_FREQUENCY"))
            )
            print(":" * 40)
            print(f'MOST FREQUENTED ITEM BY EACH DATE USING POLARS LAZYFRAMES:\n{pol_ranks.collect()}')

            query = """
                    WITH 
                        FREQUENCIES AS (
                            SELECT
                                DATES, 
                                ITEM, 
                                COUNT(*) AS FREQUENCY 
                            FROM 
                                ITEMS_03
                            GROUP BY 
                                DATES, 
                                ITEM),
                        RANKS AS (
                            SELECT
                                DATES, 
                                ITEM, 
                                RANK() OVER (PARTITION BY 
                                                 DATES 
                                             ORDER BY 
                                                 FREQUENCY DESC) AS RANKED 
                            FROM 
                                FREQUENCIES) 
            SELECT 
                DATES, 
                ITEM
            FROM 
                RANKS
            WHERE 
                RANKED = 1
            ORDER BY
                1
            """
            result = arrowkit.get_ArrowTable(conn, query)
            df = result.to_pandas()
            print(":" * 40)
            print(f'MOST FREQUENTED ITEM BY EACH DATE USING QUERIES:\n{result}')
            print(f'<*pandas visualization*>\n{df}')
 
        finally:

            conn.close()

    else:

        print(f'TABLE {table} NOT AVAILABLE')
