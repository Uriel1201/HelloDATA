begin

    using Pkg
    Pkg.add(["DataFrames", "Arrow", "DuckDB", "SQLite"])
    using DataFrames
    using Arrow
    using DuckDB
    using SQLite

end
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
    only = ["USERS"]
    list_tables = collect(SQLite.tables(db))
    names = [t.name for t in list_tables]

    if name in names

        return true

    elseif name in only

        schema = Tables.Schema((:USER_ID, :ACTION, :DATES), (Int32, String, String))
        SQLite.createtable!(db, "USERS", schema, temp = false)

        rows = [(1, "start", "01-jan-20"),
                (1, "cancel", "02-jan-20"),
                (2, "start", "03-jan-20"),
                (2, "publish", "04-jan-20"),
                (3, "start", "05-jan-20"),
                (3, "cancel", "06-jan-20" ),
                (1, "start", "07-jan-20"),
                (1, "publish", "08-jan-20")]

        placeholders = join(["(?, ?, ?)" for _ in rows], ", ")
        query = "INSERT INTO USERS (USER_ID, ACTION, DATES) VALUES $placeholders"
        stmt = SQLite.Stmt(db, query)
        params = collect(Iterators.flatten(rows))
        DBInterface.execute(stmt, params)


        return true

    else

        return false

    end

end
#=
**********************************************
=#
function get_Arrow(db::SQLite.DB, table::String)::Arrow.Table

    name = uppercase(table)
    query = DBInterface.execute(db, "SELECT * FROM $name")
    io = IOBuffer()
    Arrow.write(io, query)
    seekstart(io)
    arrow_table = Arrow.Table(io)

    @info "arrow table created"
    return arrow_table

end
#=
**********************************************
=#
function main(args)

    datab = "my_SQLite.db"
    db = SQLite.DB(datab)

    if is_available(db, args)

        SQLite.close(db)
        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")
            config = DatabaseConfig("my_SQLite.db")
            arrow_users = sqlite_connection(config) do db

                get_Arrow(db, args)

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

            users = arrow_users |> DataFrame
            sample = first(users, 5)
            println("\n", "*"^40)
            println("\nARROW_USERS AS A DATAFRAME LECTURE -> SAMPLE:\n$sample")

            start_df = time()
            dummy = select(users, :USER_ID, [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(users.ACTION)])
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

        println("$args TABLE NOT AVAILABLE")

    end

end
