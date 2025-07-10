const DB_PATH = "my_SQLite.db"
using .MyDataBase, DataFrames, StatsBase, Arrow, SQLite, DuckDB, .SQLiteArrowKit, PrettyTables 

MyDataBase.main()
#=
**********************************************
=#
function print_DuckTable(cursor::DuckDB.QueryResult)

    pretty_table(
                 cursor;
                 tf = tf_unicode,
                 hlines = [:begin, 1],
                 vlines = [1]
    )
end
#=
**********************************************
=#
function main(args = ARGS)

    db = SQLite.DB(DB_PATH)
    name = uppercase(args)
    if is_available(db, args) && (name == "ITEMS_03")

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            arrow_items = get_ArrowTable(db, args)
            println("RETURNING TABLE ITEMS_03 FROM DATABASE:\n")
            println(arrow_items)
            DuckDB.register_data_frame(duck, arrow_items, "arrow_items")
            duck_items = DBInterface.execute(duck, "SELECT * FROM 'arrow_items' USING SAMPLE 50% (bernoulli)")
            println("\n", "*"^40)
            println("ITEMS TABLE (DuckDB) -> SAMPLE:")
            print_DuckTable(duck_items)
            query = """
            WITH 
                FREQUENCIES AS (
                    SELECT
                        DATES, 
                        ITEM, 
                        COUNT(*) AS FREQUENCY 
                    FROM 
                        'arrow_items'
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
            duck_result = DBInterface.execute(duck, query)
            println("\n", "*"^40)
            println("MOST FREQUENTED ITEM BY EACH DATE (DuckDB):")
            print_DuckTable(duck_result)

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
            println("MOST FREQUENTED ITEM BY EACH DATE (DataFrames.jl):\n$filtered")
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
