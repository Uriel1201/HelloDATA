using SQLite, Tables

module MyDataBase
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
    @info "SQLite: Connection Open"

    try

        return f(db)

    catch e

        @error "Error in operation" exception=(e, catch_backtrace())
        rethrow()

    finally

        SQLite.close(db)
        @info "SQLite: Connection Closed"

    end

end
#=
**********************************************
=#
function users_01(db::SQLite.DB)

    schema = Tables.Schema((:USER_ID, :ACTION, :DATES), (Int32, String, String))
    SQLite.createtable!(db, "USERS_01", schema, temp = false)

    rows = [(1, "start", "01-jan-20"),
            (1, "cancel", "02-jan-20"),
            (2, "start", "03-jan-20"),
            (2, "publish", "04-jan-20"),
            (3, "start", "05-jan-20"),
            (3, "cancel", "06-jan-20" ),
            (1, "start", "07-jan-20"),
            (1, "publish", "08-jan-20")]

    placeholders = join(["(?, ?, ?)" for _ in rows], ", ")
    query = "INSERT INTO USERS_01 (USER_ID, ACTION, DATES) VALUES $placeholders"
    stmt = SQLite.Stmt(db, query)
    params = collect(Iterators.flatten(rows))
    DBInterface.execute(stmt, params)

    columns = join(schema.names, " | ")
    @info "TABLE USERS_01 CREATED:" columns

end
#=
**********************************************
=#
function transactions_02(db::SQLite.DB)

    schema = Tables.Schema((:SENDER, :RECEIVER, :AMOUNT, :TRANSACTION_DATE), (Int32, Int32, Float64, String))
    SQLite.createtable!(db, "TRANSACTIONS_02" , schema, temp = false)

    rows = [(5, 2, 10.0, "12-feb-20"),
            (1, 3, 15.0, "13-feb-20"),
            (2, 1, 20.0, "13-feb-20"),
            (2, 3, 25.0, "14-feb-20"),
            (3, 1, 20.0, "15-feb-20"),
            (3, 2, 15.0, "15-feb-20"),
            (1, 4, 5.0, "16-feb-20")]

    placeholders = join(["(?, ?, ?, ?)" for _ in rows], ", ")
    query = "INSERT INTO TRANSACTIONS_02 (SENDER, RECEIVER, AMOUNT, TRANSACTION_DATE) VALUES $placeholders"
    stmt = SQLite.Stmt(db, query)
    params = collect(Iterators.flatten(rows))
    DBInterface.execute(stmt, params)

    columns = join(schema.names, " | ")
    @info "TABLE TRANSACTIONS_02 CREATED:" columns

end
#=
**********************************************
=#
function items_03(db::SQLite.DB)

    schema = Tables.Schema((:DATES, :ITEM), (String, String))
    SQLite.createtable!(db, "ITEMS_03" , schema, temp = false)
    
    rows = [("01-jan-20",
             "apple"),
            ("01-jan-20",
             "apple"),
            ("01-jan-20",
             "pear"),
            ("01-jan-20",
             "pear"),
            ("02-jan-20",
             "pear"),
            ("02-jan-20",
             "pear"),
            ("02-jan-20",
             "pear"),
            ("02-jan-20",
             "orange")]
    
    placeholders = join(["(?, ?)" for _ in rows], ", ")
    query = "INSERT INTO ITEMS_03 (DATES, ITEM) VALUES $placeholders"
    stmt = SQLite.Stmt(db, query)
    params = collect(Iterators.flatten(rows))
    DBInterface.execute(stmt, params)
    
    columns = join(schema.names, " | ")
    @info "TABLE ITEMS_03 CREATED:" columns

end
#=
**********************************************
=#
function main()

    config = DatabaseConfig("my_SQLite.db")
    sqlite_connection(config) do db

        users_01(db)
        transactions_02(db)
        items_03(db)

    end

end
#=
**********************************************
=#
end
