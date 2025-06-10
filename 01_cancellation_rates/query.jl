#=
using Pkg

packages = ["SQLite", "Tables", "DataFrames", "Arrow", "Downloads", "DuckDB"]

for pkg in packages

    Pkg.add(pkg)

end
=#

DB_PATH = "my_SQLite.db"

using Downloads

arrowkit = "https://github.com/Uriel1201/HelloDATA/raw/refs/heads/main/SQLiteArrowKit.jl"
Downloads.download(arrowkit,"arrowkit.jl")
db_url = "https://github.com/Uriel1201/HelloDATA/raw/refs/heads/main/my_SQLite.jl"
Downloads.download(db_url, "database.jl")

include("database.jl")
include("arrowkit.jl")

using .MyDataBase, DataFrames, Arrow, SQLite, DuckDB, .SQLiteArrowKit

MyDataBase.main()
#=
**********************************************
=#
function main(args = ARGS)

    db = SQLite.DB(DB_PATH)

    if is_available(db, args)

        df_users = SQLiteArrowKit.get_DataFrame(db, args)
        duck = nothing

        try

            duck = DBInterface.connect(DuckDB.DB, ":memory:")

            arrow_users = SQLiteArrowKit.get_ArrowTable(db, args)
            DuckDB.register_data_frame(duck, arrow_users, "USERS")
            duck_users = DBInterface.execute(duck, "SELECT * FROM USERS USING SAMPLE 50% (bernoulli)") |> DataFrame
            println("\n", "*"^40)
            println("\nUSERS TABLE USING DUCKDB QUERIES -> SAMPLE:\n$duck_users")

            query = """
            WITH DUCK_UPDATED AS (
            SELECT
                USER_ID,
                ACTION,
                STRFTIME(STRPTIME(DATES, '%d-%b-%y'), '%Y-%m-%d')::DATE AS DATES
            FROM USERS),
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
            println("\n", "*"^40)
            println("\nUSER STATISTICS USING DUCKDB QUERIES:\n$duck_result")

            sample = first(df_users, 5)
            println("\n", "*"^40)
            println("\nUSERS DATAFRAME -> SAMPLE:\n$sample")

            dummy = select(df_users, :USER_ID, [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(df_users.ACTION)])
            totals = combine(groupby(dummy, :USER_ID), names(dummy, Not(:USER_ID)) .=> sum)
            totals.CANCEL_RATE = @.ifelse(totals.start_sum != 0, totals.cancel_sum ./ totals.start_sum, 0.0)
            totals.PUBLISH_RATE = @.ifelse(totals.start_sum != 0, totals.publish_sum ./ totals.start_sum, 0.0)
            result = select(totals, :USER_ID, :CANCEL_RATE, :PUBLISH_RATE)
            
            println("\n", "*"^40)
            println("\nUSER STATISTICS USING DATAFRAMES TOOLS:\n EXECUTION TIME:\n$result")

        finally

            DBInterface.close!(duck)

        end

    else

        println("$args TABLE NOT AVAILABLE IN $DB_PATH")

    end

end
#=
**********************************************
=#
if Base.@isdefined(PROGRAM_FILE) &&
   abspath(PROGRAM_FILE) == abspath(@__FILE__)

    main()

end
