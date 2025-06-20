const DB_PATH = "my_SQLite.db"
using .MyDataBase, DataFrames, Arrow, SQLite, DuckDB, .SQLiteArrowKit

MyDataBase.main()
#=
**********************************************
=#
function main(args = ARGS)

    db = SQLite.DB(DB_PATH)

    if SQLiteArrowKit.is_available(db, args) && (args == "transactions_02")

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            arrow_transactions = SQLiteArrowKit.get_ArrowTable(db, args)
            DuckDB.register_data_frame(duck, arrow_transactions, "TRANSACTIONS")
            duck_sample = DBInterface.execute(duck, "SELECT * FROM TRANSACTIONS USING SAMPLE 50% (bernoulli)") |> DataFrame
            println("*"^40)
            println("TRANSACTIONS TABLE USING DUCKDB QUERIES -> SAMPLE:\n$duck_sample")

            query = """
            WITH SENDERS AS (
                SELECT
                    SENDER,
                    SUM(AMOUNT) AS SENDED
                FROM
                    TRANSACTIONS
                GROUP BY
                    SENDER),
            RECEIVERS AS (
                SELECT
                    RECEIVER,
                    SUM(AMOUNT) AS RECEIVED
                FROM
                    TRANSACTIONS 
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
            
            println("\n", "*"^40)
            println("NET CHANGES USING DUCKDB QUERIES:\n$duck_query")

            df_transactions = arrow_transactions |> DataFrame

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
            println("\n", "*"^40)
            println("NET CHANGES USING DATAFRAMES:\n$result")

        finally

            DBInterface.close!(duck)

        end

    else

        println(" TABLE $args not AVAILABLE")

    end

end
#=
**********************************************
=#
if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    main()

end
