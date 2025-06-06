begin

    using Pkg
    Pkg.add(["DataFrames", "Arrow", "DuckDB", "SQLite", "Downloads"])
    using DataFrames
    using Arrow
    using DuckDB
    using SQLite
    using Downloads

end

url = "https://github.com/Uriel1201/HelloDATA/raw/refs/heads/main/my_SQLite.db"
db_path = "my_SQLite.db"
Downloads.download(url, db_path)
#=
**********************************************
=#
struct DatabaseConfig

    db_path::String

end
#=
**********************************************
=#
function sqlite_connection(f::Function, config::DatabaseConfig)

    db = SQLite.DB(config.db_path)
    @info "SQLite: open connection"

    try

        return f(db)

    catch e

        @error "Error in operation" exception=(e, catch_backtrace())
        rethrow()

    finally

        SQLite.close(db)
        @info "SQLite: closed connection"

    end

end
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
    Arrow.write(io, DBInterface.execute(db, "SELECT * FROM $name"))
    seekstart(io)
    arrow_table = Arrow.Table(io)

    @info "arrow table created"
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
