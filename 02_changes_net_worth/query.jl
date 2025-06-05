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
    @info "SQLite: Connection open"

    try

        return f(db)

    catch e

        @error "Error in operation" exception=(e, catch_backtrace())
        rethrow()

    finally

        SQLite.close(db)
        @info "SQLite: Connection closed"

    end

end
#=
**********************************************
=#
function is_available(db::SQLite.DB, table::String)::Bool

    name = uppercase(table)
    only = ["TRANSACTIONS_02"]
    list_tables = collect(SQLite.tables(db))
    names = [t.name for t in list_tables]

    if name in names

        return true

    elseif name in only

        schema = Tables.Schema((:SENDER, :RECEIVER, :AMOUNT, :TRANSACTION_DATE), (Int32, Int32, Float64, String))
        SQLite.createtable!(db, name, schema, temp = false)

        rows = [(5, 2, 10.0, "12-feb-20"),
                (1, 3, 15.0, "13-feb-20"),
                (2, 1, 20.0, "13-feb-20"),
                (2, 3, 25.0, "14-feb-20"),
                (3, 1, 20.0, "15-feb-20"),
                (3, 2, 15.0, "15-feb-20"),
                (1, 4, 5.0, "16-feb-20")]

        placeholders = join(["(?, ?, ?, ?)" for _ in rows], ", ")
        query = "INSERT INTO $name (SENDER, RECEIVER, AMOUNT, TRANSACTION_DATE) VALUES $placeholders"
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

    db = SQLite.DB(db_path)

    if is_available(db, args)

        SQLite.close(db)
        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")
            config = DatabaseConfig(db_path)
            arrow_transactions = sqlite_connection(config) do db

                get_Arrow(db, args)

            end

            DuckDB.register_data_frame(duck, arrow_transactions, "duck_transactions")
            duck_sample = DBInterface.execute(duck, "SELECT * FROM 'duck_transactions' USING SAMPLE 50% (bernoulli)") |> DataFrame
            println("\n", "*"^40)
            println("\nARROW_TRANSACTIONS USING DUCK QUERIES -> SAMPLE:\n$duck_sample")

            start_duck = time()
            query = """
            WITH SENDERS AS (
                SELECT
                    SENDER,
                    SUM(AMOUNT) AS SENDED
                FROM
                    'duck_transactions'
                GROUP BY
                    SENDER),
            RECEIVERS AS (
                SELECT
                    RECEIVER,
                    SUM(AMOUNT) AS RECEIVED
                FROM
                    'duck_transactions'
                GROUP BY
                    RECEIVER) SELECT
                                  COALESCE(S.SENDER, R.RECEIVER) AS USER_ID,
                                  COALESCE(R.RECEIVED, 0) - COALESCE(S.SENDED, 0) AS NET_CHANGE
                              FROM
                                  RECEIVERS R
                              FULL JOIN SENDERS S ON (R.RECEIVER = S.SENDER)
                              ORDER BY 2 DESC
            """
            duck_query = DBInterface.execute(duck, query) |> DataFrame
            end_duck = time()
            elapsed_duck = end_duck - start_duck
            println("\n", "*"^40)
            println("\nNET CHANGES USING DUCK QUERIES:\n EXECUTION TIME: $elapsed_duck\n$duck_query\n")

            df_transactions = arrow_transactions |> DataFrame
            sample = first(df_transactions, 5)
            println("\n", "*"^40)
            println("\nARROW_TRANSACTIONS AS A DATAFRAME LECTURE -> SAMPLE:\n$sample")

            start_df = time()
            df = (rename!(stack(select(df_transactions,
                                       :AMOUNT,
                                       :SENDER,
                                       :RECEIVER
                                      ),
                                2:3
                               ),
                          :variable => :TYPE,
                          :value => :USER_ID
                         )
            )
            transform!(df, [:TYPE, :AMOUNT] => ByRow((x, y) -> x == "SENDER" ? -1 * y : y) => :AMOUNT)

            result = (sort(combine(groupby(df,
                                           :USER_ID
                                          ),
                                   :AMOUNT => sum
                                  ),
                           :AMOUNT_sum,
                           rev = true
                          )
            )
            end_df = time()
            elapsed_df = end_df - start_df
            println("\n", "*"^40)
            println("\nNET CHANGES USING DATAFRAMES TOOLS:\n EXECUTION TIME: $elapsed_df\n$result\n")

        finally

            DBInterface.close!(duck)

        end

    else

        println("$args TABLE NOT AVAILABLE in $db_path")

    end

end
