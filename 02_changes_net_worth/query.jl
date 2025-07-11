const DB_PATH = "my_SQLite.db"
using .MyDataBase, DataFrames, Arrow, SQLite, DuckDB, .SQLiteArrowKit, PrettyTables 

MyDataBase.main()
#=
**********************************************
=#
function print_DuckTable(cursor::DuckDB.QueryResult)

    pretty_table(
                 cursor;
                 tf = tf_unicode,
                 hlines = [:begin, 1],
                 vlines = [0, :end]
    )
end
#=
**********************************************
=#
function main(args = ARGS)

    db = SQLite.DB(DB_PATH)
    name = uppercase(args)

    if is_available(db, args) && (name == "TRANSACTIONS_02")

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")
            arrow_transactions = get_ArrowTable(db, args)
            println("RETURNING TABLE TRANSACTIONS_02 FROM DATABASE:\n$arrow_transactions")

            DuckDB.register_data_frame(duck, arrow_transactions, "arrow_transactions")
            sample = """
            SELECT 
                * 
            FROM 
                'arrow_transactions'
            USING 
                SAMPLE 50% (bernoulli)
            """
            duck_sample = DBInterface.execute(duck, sample)
            println("\n", "*"^40)
            println("TRANSACTIONS TABLE (DuckDB) -> SAMPLE:")
            print_DuckTable(duck_sample)

            query = """
            WITH SENDERS AS (
                SELECT
                    SENDER,
                    SUM(AMOUNT) AS SENDED
                FROM
                    'arrow_transactions'
                GROUP BY
                    SENDER),
            RECEIVERS AS (
                SELECT
                    RECEIVER,
                    SUM(AMOUNT) AS RECEIVED
                FROM
                    'arrow_transactions'
                GROUP BY
                    RECEIVER) SELECT
                                  COALESCE(S.SENDER, R.RECEIVER) AS USER_ID,
                                  COALESCE(R.RECEIVED, 0) - COALESCE(S.SENDED, 0) AS NET_CHANGE
                              FROM
                                  RECEIVERS R
                              FULL JOIN SENDERS S ON (R.RECEIVER = S.SENDER)
                              ORDER BY 2 DESC
            """
            duck_query = DBInterface.execute(duck, query)
            println("\n", "*"^40)
            println("NET CHANGES MADE BY EACH USER (DuckDB):")
            print_DuckTable(duck_query)
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
            println("NET CHANGES MADE BY EACH USER (DataFrames.jl):\n$result")

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
