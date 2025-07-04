const DB_PATH = "my_SQLite.db"
using .MyDataBase, DataFrames, StatsBase, Arrow, SQLite, DuckDB, .SQLiteArrowKit

MyDataBase.main()
#=
**********************************************
=#
function main(args = ARGS)

    db = SQLite.DB(DB_PATH)

    if is_available(db, args) && (args == "items_03")

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            arrow_items = SQLiteArrowKit.get_ArrowTable(db, args)
            DuckDB.register_data_frame(duck, arrow_items, "ITEMS")
            duck_items = DBInterface.execute(duck, "SELECT * FROM ITEMS USING SAMPLE 50% (bernoulli)") |> DataFrame
            println("\n", "*"^40)
            println("ITEMS TABLE USING DUCKDB QUERIES -> SAMPLE:\n$duck_items")

            query = """
            WITH
                FREQUENCIES AS (
                    SELECT
                        DATES,
                        ITEM,
                        COUNT(*) AS FREQUENCY
                    FROM
                        ITEMS
                    GROUP BY
                        DATES,
                        ITEM),
                RANKS AS (
                    SELECT
                        DATES,
                        ITEM,
                        RANK() OVER (PARTITION BY
                                       DATES
                                     ORDER BY
                                       FREQUENCY DESC) AS RANKED
                    FROM
                        FREQUENCIES)
            SELECT
                DATES,
                ITEM
            FROM
                RANKS
            WHERE
                RANKED = 1
            ORDER BY
                1
            """
            duck_result = DBInterface.execute(duck, query) |> DataFrame
            println("\n", "*"^40)
            println("MOST FREQUENTED ITEM BY EACH DATE USING DUCKDB QUERIES:\n$duck_result")

            items = arrow_items |> DataFrame
            frequencies = combine(groupby(items,
                                          [:DATES, :ITEM]
                                  ),
                                  nrow => :FREQUENCIES 
                          )
            transform!(groupby(frequencies,
                               :DATES
                       ),
                       :FREQUENCIES => (x -> competerank(x, rev = true)) => :RANK
            )
            filtered = filter(row -> row.RANK == 1, 
                              frequencies 
                       )
            println("\n", "*"^40)
            println("MOST FREQUENTED ITEM BY EACH DATE USING DATAFRAMES:\n$filtered")
        finally

            DBInterface.close!(duck)

        end

    else

        println("TABLE $args NOT AVAILABLE")

    end

end
#=
**********************************************
=#
if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    main()

end
