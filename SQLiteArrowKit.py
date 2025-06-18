#python -m pip install adbc_driver_sqlite duckdb --upgrade
import adbc_driver_sqlite.dbapi as dbapi
import polars as pol
import pyarrow as pa
import duckdb
import time

#============================================================
def is_available(conn:dbapi.AdbcSqliteConnection, table:str) -> bool:

    name = table.upper()
    catalog = conn.adbc_get_objects().read_all().to_pylist()[0]
    tables = catalog["catalog_db_schemas"][0]["db_schema_tables"]
    names = [item["table_name"] for item in tables]

    return name in names
#============================================================
def get_ArrowTable(conn:dbapi.AdbcSqliteConnection, table:str) -> pa.Table:

    name = table.upper()

    with conn.cursor() as cursor:
        
        cursor.execute("SELECT * FROM " + name)
        return cursor.fetch_arrow_table()
#============================================================
#THIS FUNCTION IS ONLY A TEST AND IT MUSTN'T BE EXECUTED
def main():

    print(f'DATA SIZE~437 MB')
    print("\n" + ":" * 50)

    start_csv = time.time()
    csv_path = "/content/drive/MyDrive/songs.csv"
    songs_csv = pol.scan_csv(csv_path,
                             has_header = True,
                             infer_schema_length = 1000,
                             ignore_errors = False
    ).collect()
    end_csv = time.time()
    elapsed_csv = round(end_csv - start_csv, 4)
    print(f'LECTURE TIME, FROM CSV TO POLARS (EAGER MODE):\ntime~{elapsed_csv}')
    print(f'PROCESSING TIME, USING POLARS (EAGER MODE):')
    start_polars = time.time()
    polMode = (songs_csv.group_by("mode")
                        .agg(pol.len().alias("frequency"))
    )
    polModePopularity = (songs_csv.filter(pol.col("popularity") > 95)
                                  .group_by(["spotify_id", "name", "mode"])
                                  .agg(pol.len().alias("persistenceInPopularity>95"))
                                  .filter(pol.col("persistenceInPopularity>95") > 5000)
    ).head(10)
   
    end_polars = time.time()
    elap_polars = round(end_polars - start_polars, 4)
    print(f'\n{polMode}')
    print(f'\n{polModePopularity}')
    print(f'\ntime~{elap_polars}')

    conn = dbapi.connect("file:/content/drive/MyDrive/popular.db?mode=ro")
    if is_available(conn, "songs"):

        try:

            duck = duckdb.connect(":memory:")
            print("\n" + ":" * 50)
            start_arrow = time.time()
            arrow_songs = get_ArrowTable(conn, "songs")
            end_arrow  = time.time()
            elap_arr = round(end_arrow - start_arrow, 4)
            print(f'LECTURE TIME, FROM CURSOR (NATIVE ARROW) TO ARROW TABLE: {elap_arr}')

            print(f'PROCESSING TIME, USING DUCKDB QUERIES:')
            duck_start = time.time()
            mode =  """
                    SELECT
                        mode,
                        COUNT(*) AS frequency
                    FROM
                        arrow_songs
                    GROUP BY
                        mode
            """
            modePopularity = """
                             WITH
                                 POPULAR AS(
                                     SELECT
                                         name,
                                         mode,
                                         COUNT(*) AS persistenceInPopularity95
                                     FROM
                                         arrow_songs
                                     WHERE
                                         popularity > 95
                                     GROUP BY
                                         spotify_id, name, mode)
                             SELECT
                                 *
                             FROM
                                 POPULAR
                             WHERE
                                 persistenceInPopularity95 > 5000
            """
            duck.sql(mode).show()
            duck.sql(modePopularity).show()
            duck_end = time.time()
            elap_duck = round(duck_end - duck_start, 4)
            print(f'time~{elap_duck}')

        finally:

            duck.close()
            conn.close()
#============================================================
if __name__ == '__main__':
    main()
