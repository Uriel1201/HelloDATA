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
    time_writing = round(end_write - start_write, digits = 4)
    time_reading = round(end_arrow - start_arrow, digits = 4)
    @info "arrow table created:" time_writing time_reading
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

    db = SQLite.DB(DB_PATH)
    csv_path = "heart_2022_no_nans.csv"

    csvstart = time()
    heart_df = CSV.read(csv_path, DataFrame)
    csvend = time()
    heart_state = combine(groupby(heart_df, :State), nrow => :Persons)
    heart_sex = combine(groupby(heart_df, :Sex), nrow => :Persons)
    sex_state = combine(groupby(heart_df, [:State, :Sex]), nrow => :Persons)
    sample_1 = first(heart_state, 10)
    sample_2 = first(sex_state, 10)
    csv_end = time()
    import_csv = round(csvend - csvstart, digits = 4)
    exec_csv = round(csv_end - csvend, digits = 4)
    println("\n", "*"^40)
    println("TIME READING CSV: $import_csv\nPROCESSING TIME (DATAFRAMES): $exec_csv")
    println("\n$heart_sex\n")
    println("\n$sample_1\n")
    println("\n$sample_2\n")

        if is_available(db, "heart_2022")

            duck = nothing

            try

                duck = DBInterface.connect(DuckDB.DB, ":memory:")
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
                import_arrow = round(arrowend - arrowstart, digits = 4)
                exec_arrow = round(arrow_end - arrowend, digits = 4)
                println("\n", "*"^40)
                println("TIME READING AND WRITING DataBaseTable => ArrowTable: $import_arrow\nPROCESSING TIME (DUCKDB QUERIES): $exec_arrow")
                println("\n$sex_duck\n")
                println("\n$state_duck\n")
                println("\n$statesex_duck\n")

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
