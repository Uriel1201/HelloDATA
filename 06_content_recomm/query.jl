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
                 vlines = [1]
    )
end
#=
**********************************************
=#
function main(table1::String, table2::String)

    db = SQLite.DB(DB_PATH)
    name1 = uppercase(table1)
    name2 = uppercase(table2)
    names = [name1, name2]
    tables = ["FRIENDS_06", "LIKES_06"]
    if is_available(db, table1) && is_available(db, table2) && (all(c -> in(c, tables), names))

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            arrow_friends = get_ArrowTable(db, table1)
            arrow_likes = get_ArrowTable(db, table2)
            println("RETURNING TABLE $table1 FROM DATABASE:")
            println(arrow_friends)
            println("\nRETURNING TABLE $table2 FROM DATABASE:")
            println(arrow_likes)

            DuckDB.register_data_frame(duck, arrow_friends, "arrow_friends")
            DuckDB.register_data_frame(duck, arrow_likes, "arrow_likes")
            println("\n", "*"^40)
            println("FRIENDS TABLE (DUCKDB QUERIES) -> SAMPLE:\n")
            sample1 = """
                     SELECT
                         *
                     FROM
                         'arrow_friends' -- arrow_friends is an arrow table
                     USING SAMPLE
                         50% (bernoulli)
            """
            duck_sample1 = DBInterface.execute(duck, sample1)
            print_DuckTable(duck_sample1)
            println("\n", "*"^40)
            println("LIKES TABLE (DUCKDB QUERIES) -> SAMPLE:\n")
            sample2 = """
                      SELECT
                          *
                      FROM
                          'arrow_likes' -- arrow_likes is an arrow table
                      USING SAMPLE
                         50% (bernoulli)
            """
            duck_sample2 = DBInterface.execute(duck, sample2)
            print_DuckTable(duck_sample2)

            println("\n", "*"^40)
            println("RETURNING RECOMMENDATIONS FOR EACH USER (DUCKDB QUERIES):\n")
            query = """
                    WITH
                        RECOMMENDATIONS AS (
                            SELECT
                                F.USER_ID,
                                L.PAGE_LIKES AS RECOMMENDATION
                            FROM
                                'arrow_friends' F
                                INNER JOIN
                                    'arrow_likes' L
                                ON
                                    F.FRIEND = L.USER_ID)
            SELECT DISTINCT
                R.USER_ID,
                R.RECOMMENDATION
            FROM
                RECOMMENDATIONS R
                ANTI JOIN
                    'arrow_likes' L
                ON
                    R.USER_ID = L.USER_ID
                AND
                    R.RECOMMENDATION = L.PAGE_LIKES
            ORDER BY
                1, 2
            """
            duck_result = DBInterface.execute(duck, query)
            print_DuckTable(duck_result)

            friends = arrow_friends |> DataFrame
            likes = arrow_likes |> DataFrame

            result = unique(select(antijoin(innerjoin(friends,
                                                      likes,
                                                      on = :FRIEND => :USER_ID
                                            ),
                                            likes,
                                            on = [:USER_ID, :PAGE_LIKES]
                                   ),
                                   :USER_ID,
                                   :PAGE_LIKES
                            )
                     )
            rename!(result, :PAGE_LIKES => :RECOMMENDATION)
            println("\n", "*"^40)
            println("RETURNING RECOMMENDATIONS FOR EACH USER (DATAFRAMES.jl):\n")
            println("$result")

        finally

            DBInterface.close!(duck)

        end

    else

        println("ANY TABLE NOT VALIDATED")

    end

end
#=
**********************************************
=#
if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    a = ARGS[1]
    b = ARGS[2]
    main(a, b)

end
