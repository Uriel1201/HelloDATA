module MyDataBase

using SQLite, Tables, Main.SQLiteArrowKit
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

    if !SQLiteArrowKit.is_available(db, "users_01")

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
        _type = join(schema.types, " | ")
        @info "TABLE USERS_01 IS AVAILABLE:" columns _type

    else

        @info "TABLE USERS_01 ALREADY EXISTS"

    end

end
#=
**********************************************
=#
function transactions_02(db::SQLite.DB)

    if !SQLiteArrowKit.is_available(db, "transactions_02")
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
        _type = join(schema.types, " | ")
        @info "TABLE TRANSACTIONS_02 IS AVAILABLE:" columns _type

    else

        @info "TABLE TRANSACTIONS_02 ALREADY EXISTS"

    end

end
#=
**********************************************
=#
function items_03(db::SQLite.DB)

    if !SQLiteArrowKit.is_available(db, "items_03")
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
        _type = join(schema.types, " | ")
        @info "TABLE ITEMS_03 IS AVAILABLE:" columns _type

    else

        @info "TABLE ITEMS_03 ALREADY EXISTS"

    end

end
#=
**********************************************
=#
function users_04(db::SQLite.DB)

    if !SQLiteArrowKit.is_available(db, "users_04")
        schema = Tables.Schema((:ID, :ACTIONS, :ACTION_DATE), (Int32, String, String))
        SQLite.createtable!(db, "USERS_04" , schema, temp = false)

        rows = [(1,
                 "Start",
                 "13-feb-20"),

                (1,
                 "Cancel",
                 "13-feb-20"),

                (2,
                 "Start",
                 "11-feb-20"),

                (2,
                 "Publish",
                 "14-feb-20"),

                (3,
                 "Start",
                 "15-feb-20"),

                (3,
                 "Cancel",
                 "15-feb-20"),

                (4,
                 "Start",
                 "18-feb-20"),

                (1,
                 "Publish",
                 "19-feb-20")]

        placeholders = join(["(?, ?, ?)" for _ in rows], ", ")
        query = "INSERT INTO USERS_04 (ID, ACTIONS, ACTION_DATE) VALUES $placeholders"
        stmt = SQLite.Stmt(db, query)
        params = collect(Iterators.flatten(rows))
        DBInterface.execute(stmt, params)

        columns = join(schema.names, " | ")
        _type = join(schema.types, " | ")
        @info "TABLE USERS_04 IS AVAILABLE:" columns _type

    else

        @info "TABLE USERS_04 ALREADY EXISTS"

    end

end
#=
**********************************************
=#
function users_05(db::SQLite.DB)

    if !SQLiteArrowKit.is_available(db, "users_05")
        schema = Tables.Schema((:USER_ID, :PRODUCT_ID, :TRANSACTION_DATE), (Int32, Int32, String))
        SQLite.createtable!(db, "USERS_05" , schema, temp = false)

        rows = [(1,
                 101,
                 "12-feb-20"),

                (2,
                 105,
                 "13-feb-20"),

                (1,
                 111,
                 "14-feb-20"),

                (3,
                 121,
                 "15-feb-20"),

                (1,
                 101,
                 "16-feb-20"),

                (2,
                 105,
                 "17-feb-20"),

                (4,
                 101,
                 "16-feb-20"),

                (3,
                 105,
                 "15-feb-20")]

        placeholders = join(["(?, ?, ?)" for _ in rows], ", ")
        query = "INSERT INTO USERS_05 (USER_ID, PRODUCT_ID, TRANSACTION_DATE) VALUES $placeholders"
        stmt = SQLite.Stmt(db, query)
        params = collect(Iterators.flatten(rows))
        DBInterface.execute(stmt, params)

        columns = join(schema.names, " | ")
        _type = join(schema.types, " | ")
        @info "TABLE USERS_05 IS AVAILABLE:" columns _type

    else

        @info "TABLE USERS_05 ALREADY EXISTS"

    end

end
#=
**********************************************
=#
function friendship_06(db::SQLite.DB)

    if !is_available(db, "friends_06")
        schema = Tables.Schema((:USER_ID, :FRIEND), (Int32, Int32))
        SQLite.createtable!(db, "FRIENDS_06" , schema, temp = false)

        rows = [(1,
                 2),

                (1,
                 3),

                (1,
                 4),

                (2,
                 1),

                (3,
                 1),

                (3,
                 4),

                (4,
                 1),

                (4,
                 3)]

        placeholders = join(["(?, ?)" for _ in rows], ", ")
        query = "INSERT INTO FRIENDS_06 (USER_ID, FRIEND) VALUES $placeholders"
        stmt = SQLite.Stmt(db, query)
        params = collect(Iterators.flatten(rows))
        DBInterface.execute(stmt, params)

        columns = join(schema.names, " | ")
        _type = join(schema.types, " | ")
        @info "TABLE FRIENDS_06 IS AVAILABLE:" columns _type

    else

        @info "TABLE FRIENDS_06 ALREADY EXISTS"

    end

    if !is_available(db, "likes_06")
        schema = Tables.Schema((:USER_ID, :PAGE_LIKES), (Int32, String))
        SQLite.createtable!(db, "LIKES_06" , schema, temp = false)

        rows = [

            (1,
             "A"),

            (1,
             "B"),

            (1,
             "C"),

            (2,
             "A"),

            (3,
             "B"),

            (3,
             "C"),

            (4,
             "B")]

        placeholders = join(["(?, ?)" for _ in rows], ", ")
        query = "INSERT INTO LIKES_06 (USER_ID, PAGE_LIKES) VALUES $placeholders"
        stmt = SQLite.Stmt(db, query)
        params = collect(Iterators.flatten(rows))
        DBInterface.execute(stmt, params)

        columns = join(schema.names, " | ")
        _type = join(schema.types, " | ")
        @info "TABLE LIKES_06 IS AVAILABLE:" columns _type

    else

        @info "TABLE LIKES_06 ALREADY EXISTS"

    end

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
        users_04(db)
        users_05(db)
        friendship_06(db)

    end

end
#=
**********************************************
=#
end
