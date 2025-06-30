module SQLiteArrowKit

using DataFrames, Arrow, SQLite, CSV, DuckDB

export is_available, get_ArrowTable, get_DataFrame

const DB_PATH = "/content/popular.db"
#=
**********************************************
=#
function is_available(db::SQLite.DB, table::String)::Bool

    name = uppercase(table)
    list_tables = collect(SQLite.tables(db))
    names = [t.name for t in list_tables]

    return name in names

end
#=
**********************************************
=#
function get_ArrowTable(db::SQLite.DB, table::String)::Arrow.Table

    name = uppercase(table)
    io = IOBuffer()
    cursor = DBInterface.execute(db, "SELECT * FROM $name")
    Arrow.write(io, cursor)
    seekstart(io)
    arrow_table = Arrow.Table(io)
    
    return arrow_table

end
#=
**********************************************
=#
function get_DataFrame(db::SQLite.DB, table::String)::DataFrame

    name = uppercase(table)

    return DBInterface.execute(db, "SELECT * FROM $name") |> DataFrame

end
#=
**********************************************
THIS FUNCTION IS ONLY A TEST AND IT MUSTN'T BE EXECUTED
=#
function main(csv_path::String, db_table::String)

    println("DATA SIZE~437 MB")
    println("\n", "*"^40)

    db = SQLite.DB(DB_PATH)

    csvstart = time()
    songs_csv = CSV.read(csv_path, DataFrame; header = true, delim = ',')
    csvend = time()
    lecture_csv = round(csvend - csvstart, digits = 4)
    println("LECTURE TIME, FROM CSV TO DATAFRAMES: $lecture_csv")
    println("PROCESSING TIME, USING DATAFRAMES:")

    start_df = time()
    dfMode = combine(groupby(songs_csv,
                             "mode"
                     ),
                     nrow => :frequency
             )
    dfModePopularity = combine(groupby(filter(:popularity => x -> x > 95,
                                       songs_csv
                                       ),
                                       [:spotify_id,
                                        :name,
                                        :mode]
                               ),
                               nrow => :persistenceInPopularity
                       )
    result= first(filter(:persistenceInPopularity => x -> x > 5000, dfModePopularity), 10)

    end_df = time()
    elap_df = round(end_df - start_df, digits = 4)
    println("\n$dfMode")
    println("\n$result\n")
    println("\ntime~$elap_df")

    if is_available(db, db_table)

            duck = nothing

            try

                duck = DBInterface.connect(DuckDB.DB, ":memory:")

                println("\n", "*"^40)

                arrow_start = time()
                duck_songs = get_ArrowTable(db, db_table)
                arrow_end = time()
                elap_arrow = round(arrow_end - arrow_start, digits = 4)
                println("LECTURE TIME (EXPLICIT EXPORT), FROM CURSOR TO ARROW TABLE:\n$elap_arrow")

                duck_start = time()
                DuckDB.register_data_frame(duck, duck_songs, "SONGS")
                mode =  """
                        SELECT
                            mode,
                            COUNT(*) AS frequency
                        FROM
                            SONGS
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
                                             SONGS
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
                modeDuck = DBInterface.execute(duck, mode) |> DataFrame
                popularityDuck = DBInterface.execute(duck, modePopularity) |> DataFrame
                duck_end = time()
                elap_duck = round(duck_end - duck_start, digits = 4)

                println("PROCESSING TIME, USING DUCKDB QUERIES: $elap_duck")
                println("\n$modeDuck")
                println("\n$popularityDuck")

            finally

                DBInterface.close!(duck)

            end

        end

end
#=
**********************************************
=#
end
#=
**********************************************
=#
if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    main(a, b)

end
