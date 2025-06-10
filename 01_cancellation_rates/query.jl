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

function main(args)

    db = SQLite.DB(db_path)

    if is_available(db, args)

        df_users = get_DataFrame(db, args)
        SQLite.close(db)
        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")
            config = DatabaseConfig(db_path)
            arrow_users = sqlite_connection(config) do db

                get_ArrowTable(db, args)

            end

            DuckDB.register_data_frame(duck, arrow_users, "duck_users")
            duck_users = DBInterface.execute(duck, "SELECT * FROM 'duck_users' USING SAMPLE 50% (bernoulli)") |> DataFrame
            println("\n", "*"^40)
            println("\nARROW_USERS USING DUCKDB QUERIES -> SAMPLE:\n$duck_users")

            start_duck = time()
            query = """
            WITH DUCK_UPDATED AS (
            SELECT
                USER_ID,
                ACTION,
                STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
            FROM duck_users),
            TOTALS AS (
            SELECT
                USER_ID,
                SUM(IF(ACTION = 'start',1,0)) AS TOTAL_STARTS,
                SUM(IF(ACTION = 'cancel',1,0)) AS TOTAL_CANCELS,
                SUM(IF(ACTION = 'publish',1,0)) AS TOTAL_PUBLISHES
            FROM
                DUCK_UPDATED
            GROUP BY
                USER_ID
            )
            SELECT
                USER_ID,
                ROUND(TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS, 0),
                2) AS PUBLISH_RATE,
                ROUND(TOTAL_CANCELS / NULLIF(TOTAL_STARTS, 0),
                2) AS CANCEL_RATE
            FROM
            TOTALS
            ORDER BY 1
            """
            duck_result = DBInterface.execute(duck, query) |> DataFrame
            end_duck = time()
            elapsed_duck = end_duck - start_duck
            println("\n", "*"^40)
            println("\nUSER STATISTICS USING DUCKDB QUERIES:\n EXECUTION TIME: $elapsed_duck\n$duck_result\n")

            sample = first(df_users, 5)
            println("\n", "*"^40)
            println("\nUSERS DATAFRAME -> SAMPLE:\n$sample")

            start_df = time()
            dummy = select(df_users, :USER_ID, [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(df_users.ACTION)])
            totals = combine(groupby(dummy, :USER_ID), names(dummy, Not(:USER_ID)) .=> sum)
            totals.CANCEL_RATE = @.ifelse(totals.start_sum != 0, totals.cancel_sum ./ totals.start_sum, 0.0)
            totals.PUBLISH_RATE = @.ifelse(totals.start_sum != 0, totals.publish_sum ./ totals.start_sum, 0.0)
            result = select(totals, :USER_ID, :CANCEL_RATE, :PUBLISH_RATE)
            end_df = time()
            elapsed_df = end_df - start_df
            println("\n", "*"^40)
            println("\nUSER STATISTICS USING DATAFRAMES TOOLS:\n EXECUTION TIME: $elapsed_df\n$result\n")

        finally

            DBInterface.close!(duck)

        end

    else

        println("$args TABLE NOT AVAILABLE IN $db_path")

    end

end
