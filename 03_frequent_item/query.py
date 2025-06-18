#!python -m pip install adbc_driver_sqlite duckdb --upgrade
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

    if arrowkit.is_available(conn, table) and (table == "items_03"):

        try:

            duck = duckdb.connect(":memory:")
       
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
                                'arrow_items'
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
            print(":" * 40)
            print(f'MOST FREQUENTED ITEM BY EACH DATE USING DUCKDB QUERIES:')
            duck.sql(query).show()

        finally:

            duck.close()
            conn.close()

    else:

        print(f'TABLE {table} NOT AVAILABLE')
