const DB_PATH = "my_SQLite.db"
using .MyDataBase, DataFrames, Dates, Arrow, SQLite, DuckDB, .SQLiteArrowKit, PrettyTables, StatsBase

MyDataBase.main()
#=
**********************************************
=#
function print_DuckTable(cursor::DuckDB.QueryResult)

    pretty_table(
                 cursor;
                 tf = tf_ascii_rounded
    )
end
#=
**********************************************
=#
function year_format(my_date::String)::String

    parts = split(my_date, "-")
    if length(parts) != 3

        return my_date

    end

    my_year = parts[end]
    if length(my_year) != 2

        return my_date

    end

    try

        year_2d = parse(Int, my_year)
        year_4d = year_2d >= 70 ? 1900 + year_2d : 2000 + year_2d

        return join([parts[1], parts[2], string(year_4d)], "-")

    catch

        return my_date

    end

end
#=
**********************************************
=#
function main(args = ARGS)

    db = SQLite.DB(DB_PATH)
    name = uppercase(args)
    if is_available(db, args) && (name == "USERS_05")

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            arrow_users = get_ArrowTable(db, args)
            println("RETURNING TABLE $args FROM DATABASE:\n")
            println(arrow_users)

            DuckDB.register_data_frame(duck, arrow_users, "arrow_users")
            println("\n", "*"^40)
            println("USERS TABLE (DUCKDB QUERIES) -> SAMPLE:\n")
            sample = """
                     SELECT
                         *
                     FROM
                         'arrow_users' -- arrow_users is an arrow table
                     USING SAMPLE
                         50% (bernoulli)
            """
            duck_sample = DBInterface.execute(duck, sample)
            print_DuckTable(duck_sample)

            println("\n", "*"^40)
            println("USERS BECOMING SUPERUSERS (DUCKDB QUERIES):\n")
            query = """
                    WITH
                        DUCK_FORMATTED AS (
                            SELECT
                                USER_ID,
                                DATE(STRPTIME(TRANSACTION_DATE, '%d-%b-%y')) AS TRANSACTION_DATE
                            FROM
                                'arrow_users'),
                        RANKINGS AS (
                            SELECT
                                USER_ID,
                                TRANSACTION_DATE,
                                ROW_NUMBER() OVER(PARTITION BY
                                                      USER_ID
                                                  ORDER BY
                                                      TRANSACTION_DATE) AS RANKED_DATE
                            FROM
                                DUCK_FORMATTED),
                        USER_ AS (
                            SELECT DISTINCT
                                USER_ID
                            FROM
                               'arrow_users'),
                        SUPERUSERS AS (
                            SELECT
                                USER_ID,
                                TRANSACTION_DATE AS DATE_AS_SUPER
                            FROM
                                RANKINGS
                            WHERE
                                RANKED_DATE = 2)
            SELECT
                U.USER_ID,
                S.DATE_AS_SUPER
            FROM
                USER_      U
                LEFT JOIN
                    SUPERUSERS S
                USING (USER_ID)
            ORDER BY
                2;
            """
            duck_result = DBInterface.execute(duck, query)
            print_DuckTable(duck_result)

            users = arrow_users |> DataFrame
            transform!(users,
                       :TRANSACTION_DATE => (x -> Date.(year_format.(x),
                                                        Ref(dateformat"d-u-y")
                                                  )
                                            ) => :TRANSACTION_DATE
            )
            users = select(sort(users,
                                [:USER_ID, :TRANSACTION_DATE]
                           ),
                           :USER_ID,
                           :TRANSACTION_DATE
                    )
            transform!(groupby(users,
                               :USER_ID
                       ),
                       eachindex => :DATE_RANK
            )
            result = leftjoin(unique(select(users,
                                            :USER_ID
                                     )
                              ),
                              filter(row -> row.DATE_RANK == 2,
                                     users
                              ),
                              on = :USER_ID
                     )
            println("\n", "*"^40)
            println("USERS BECOMING SUPERUSERS (DATAFRAMES.jl):\n")
            println("$result")

        finally

            DBInterface.close!(duck)

        end

    else

        println("TABLE $args NOT VALIDATED")

    end

end
#=
**********************************************
=#
if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    main()

end
