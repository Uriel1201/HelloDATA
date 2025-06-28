const DB_PATH = "my_SQLite.db"
using .MyDataBase, DataFrames, ShiftedArrays, Dates, Arrow, SQLite, DuckDB, .SQLiteArrowKit, PrettyTables

MyDataBase.main()
#=
**********************************************
=#
function print_DuckTable(cursor::DuckDB.QueryResult)

    pretty_table(
                 cursor;
                 tf = tf_mysql
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
    if is_available(db, args) && (name == "USERS_04")

        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            arrow_users = get_ArrowTable(db, args)
            println("RETURNING TABLE USERS_04 FROM DATABASE:\n")
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
            println("ELAPSED TIME BETWEEN LAST ACTIONS (DUCKDB QUERIES):\n")
            query = """
                    WITH
                        DUCK_FORMATTED AS (
                            SELECT
                                ID,
                                ACTIONS,
                                DATE(STRPTIME(ACTION_DATE, '%d-%b-%y')) AS ACTION_DATE
                            FROM
                                'arrow_users'),
                        ORDERED_DATES AS (
                            SELECT
                                ID,
                                ACTION_DATE,
                                ROW_NUMBER() OVER (PARTITION BY
                                                       ID
                                                   ORDER BY
                                                       ACTION_DATE
                                                   DESC) AS ORDERED
                            FROM
                                DUCK_FORMATTED),
                        LAST_DATES AS (
                            SELECT
                                ID,
                                ACTION_DATE AS LAST_DATE
                            FROM
                                ORDERED_DATES
                            WHERE
                                ORDERED = 1),
                        PENULTIMATE_DATES AS (
                            SELECT
                                ID,
                                ACTION_DATE AS PENULTIMATE_DATE
                            FROM
                                ORDERED_DATES
                            WHERE
                                ORDERED = 2)
            SELECT
                L.ID,
                (L.LAST_DATE - P.PENULTIMATE_DATE) AS ELAPSED_DAYS
            FROM
                LAST_DATES L
                    LEFT JOIN
                        PENULTIMATE_DATES P
                    USING (ID)
            ORDER BY
                1
            """
            duck_result = DBInterface.execute(duck, query)
            print_DuckTable(duck_result)

            users = arrow_users |> DataFrame
            transform!(users,
                       :ACTION_DATE => (x -> Date.(year_format.(x),
                                                   Ref(dateformat"d-u-y")
                                             )) => :ACTION_DATE
            )
            users = sort(users,
                         [:ID, :ACTION_DATE],
                         rev = [false, true]
                    )
            transform!(groupby(users,
                               :ID
                       ),
                       :ACTION_DATE => (x -> (x .- ShiftedArrays.lead(x, 1))) => :ELAPSED_DAYS
            )
            println("\n", "*"^40)
            println("ELAPSED TIME BETWEEN LAST ACTIONS (DATAFRAMES.jl):\n")

            users = combine(groupby(users,
                                    :ID
                            ),
                            :ELAPSED_DAYS => first => :ELAPSED_DAYS
                    )
            users.ELAPSED_DAYS = passmissing(x -> x.value).(users.ELAPSED_DAYS)
            println(users)

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
