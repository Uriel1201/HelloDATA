using Pkg
Pkg.add(["DataFrames", "Arrow", "SQLite", "Downloads", "CSV", "DuckDB"])

module SQLiteArrowKit

using DataFrames, Arrow, SQLite, Downloads, CSV, DuckDB

export sqlite_connection, is_available, get_ArrowTable, get_DataFrame

const DB_PATH = "heart.db"
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

    query = DBInterface.execute(db, "SELECT * FROM $name")
    start_write = time()
    Arrow.write(io, query)
    end_write = time()
    start_arrow = time()
    seekstart(io)
    arrow_table = Arrow.Table(io)
    end_arrow = time()
    conversion_time = round(end_write - start_write, digits = 4)
    lecture_time = round(end_arrow - start_arrow, digits = 4)
    @info "arrow table created:" conversion_time lecture_time
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
=#
function main(args = ARGS)

    println("DATA SIZE ~ 82 MB")
    println("\n", "*"^40)

    db = SQLite.DB(DB_PATH)
    csv_path = "heart_2022_no_nans.csv"

    csvstart = time()
    heart_csv = CSV.read(csv_path, DataFrame)
    csvend = time()
    lecture_csv = round(csvend - csvstart, digits = 4)
    println("\n", "*"^40)
    println("LECTURE TIME, 'CSV TO DATAFRAME': $lecture_csv")
    
        if is_available(db, "heart_2022")

            duck = nothing

            try

                duck = DBInterface.connect(DuckDB.DB, ":memory:")

                println("\n", "*"^40)
                start_cursor = time()
                cursor_df = get_DataFrame(db, "heart_2022")
                end_cursor = time()
                lecture_cursor = round(end_cursor - start_cursor, digits = 4)
                println("LECTURE TIME, 'DBInterface.Cursor() TO DATAFRAMES': $lecture_cursor")
            
                arrowstart = time()
                duck_heart = get_ArrowTable(db, "heart_2022")
                arrowend = time()
                DuckDB.register_data_frame(duck, duck_heart, "HEART")
                state = """
                        SELECT
                            State,
                            COUNT(*) AS PERSONS
                         FROM
                             'HEART'
                         GROUP BY
                             State
                         LIMIT 10
                         """
                sex = """
                      SELECT
                          Sex,
                          COUNT(*) AS PERSONS
                      FROM
                          'HEART'
                      GROUP BY
                          Sex
                      """
                sex_state = """
                            SELECT
                                State,
                                Sex,
                                COUNT(*) AS PERSONS
                            FROM
                                'HEART'
                            GROUP BY
                                State, Sex
                            LIMIT 10
                            """

                state_duck = DBInterface.execute(duck, state) |> DataFrame
                sex_duck = DBInterface.execute(duck, sex) |> DataFrame
                statesex_duck = DBInterface.execute(duck, sex_state) |> DataFrame
                arrow_end = time()
                lecture_arrow = round(arrowend - arrowstart, digits = 4)
                exec_arrow = round(arrow_end - arrowend, digits = 4)
                println("\n", "*"^40)
                println("CONVERSION-LECTURE TIME: 'DBInterface.Cursor() => DOC.ARROW => ArrowTable': $lecture_arrow\nPROCESSING TIME (DUCKDB QUERIES): $exec_arrow")
                println("\n$sex_duck\n")
                println("\n$state_duck\n")
                println("\n$statesex_duck\n")

                df_start = time()
                heart_state = combine(groupby(heart_csv, :State), nrow => :Persons)
                heart_sex = combine(groupby(heart_csv, :Sex), nrow => :Persons)
                sex_state = combine(groupby(heart_csv, [:State, :Sex]), nrow => :Persons)
                sample_1 = first(heart_state, 10)
                sample_2 = first(sex_state, 10)
                df_end = time()
                exec_df = df_end - df_start
                println("\n", "*"^40)
                println("PROCESSING TIME (ANALYSIS IN DATAFRAMES): $exec_df")
                println("\n$heart_sex\n")
                println("\n$sample_1\n")
                println("\n$sample_2\n")

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

    main()

end
