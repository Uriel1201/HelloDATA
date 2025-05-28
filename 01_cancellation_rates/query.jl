begin
    using Pkg
    Pkg.add(["DataFrames", "SQLite"])
    using DataFrames
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
function database_connection(f::Function, config::DatabaseConfig)

    db = SQLite.DB(config.db_path)
    try
        @info "Connection is established"
        f(db)
    catch e
        @error "Error in operation" exception=(e, catch_backtrace())
        rethrow()
    finally
        SQLite.close(db)
        @info "Connection is closed"
    end

end
#=
**********************************************
=#
function get_users(db::SQLite.DB)

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

    SQLite.execute(db, "BEGIN TRANSACTION")
    placeholders = join(["(?, ?, ?)" for _ in rows], ", ")
    query = "INSERT INTO USERS (USER_ID, ACTION, DATES) VALUES $placeholders"
    stmt = SQLite.Stmt(db, query)
    params = collect(Iterators.flatten(rows))
    DBInterface.execute(stmt, params)
    SQLite.execute(db, "COMMIT")
    @info "users table is available"
end
#=
**********************************************
=#
const SPACE = "********************************"

function main(args)

    database = uppercase(args)
    println("$SPACE\nWORKING WITH $database\n$SPACE\n")
    
    config = DatabaseConfig(":memory:")
    database_connection(config) do db

        get_users(db)
        users = DBInterface.execute(db, "SELECT * FROM USERS") |> DataFrame
        sample = first(users, 5)
        println("\n$SPACE")
        println("\n USERS TABLE -> SAMPLE:\n$sample")

        dummy = select(users, :USER_ID, [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(users.ACTION)])
        println("\n$SPACE")
        println("\n WAS THIS ACTION DONE BY USER -> SAMPLE:\n$dummy")

        totals = combine(groupby(dummy, :USER_ID), names(dummy, Not(:USER_ID)) .=> sum)
        totals.cancel_rate = @.ifelse(totals.start_sum != 0, totals.cancel_sum ./ totals.start_sum, 0.0)
        totals.publish_rate = @.ifelse(totals.start_sum != 0, totals.publish_sum ./ totals.start_sum, 0.0)
        result = select(totals, :USER_ID, :cancel_rate, :publish_rate)
        println("\n$SPACE")
        println("\n USER STATISTICS:\n$result\n")

    end

end
#=
**********************************************
=#
if abspath(PROGRAM_FILE) == @__FILE__

    main(ARGS)

else

    println("\n ARCHIVE LOADED AS MODULE, EXECUTE MAIN() MANUALLY WITH 'SQLITE'AS ARG.")

end
