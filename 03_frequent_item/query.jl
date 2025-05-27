begin
    using Pkg
    Pkg.add(["DataFrames", "SQLite", "Dates"])
    using DataFrames
    using SQLite
    using Dates
end

db = SQLite.DB()

#*/________________________________

schema = Tables.Schema((:ITEM, :DATES), (String, String))
SQLite.createtable!(db, "ITEMS", schema, temp = false)
rows = [("apple", "01-jan-20"),
        ("apple", "01-jan-20"),
        ("pear", "01-jan-20"),
        ("pear", "01-jan-20"),
        ("pear", "02-jan-20"),
        ("pear", "02-jan-20"),
        ("pear", "02-jan-20"),
        ("orange", "02-jan-20")]

SQLite.execute(db, "BEGIN TRANSACTION")
placeholders = join(["(?, ?)" for _ in rows], ", ")
query = "INSERT INTO ITEMS (ITEM, DATES) VALUES $placeholders"
stmt = SQLite.Stmt(db, query)
params = collect(Iterators.flatten(rows))
DBInterface.execute(stmt, params)
SQLite.execute(db, "COMMIT")

#*/________________________________

const DATE_REGEX = r"-(\d{2})"i
const SPACE = "********************************"

#*/________________________________

function format_date(match)
    year = "20" * match[2:3]
    return "-" * year
end

#*/________________________________

function main(args = String())

    database = uppercase(args[1])
    println("$SPACE\nWORKING WITH A $database DATABASE\n$SPACE")
    table = uppercase(args[2])
    items = DBInterface.execute(db, "SELECT * FROM $table") |> DataFrame
    items.DATES .= replace.(items.DATES, DATE_REGEX => format_date)
    items[!, :DATES] = Date.(items.DATES, "dd-u-yyyy")

    sample = first(items, 5)
    println("\n$SPACE")
    println("ITEMS TABLE -> SAMPLE:\n$sample")

    df = combine(groupby(sample,
                         [:DATES, :ITEM]
                 ),
                 :ITEM => length => :COUNT
         )
    println("\n$SPACE")
    println("Number of items by each date -> SAMPLE:\n$df")

    max_items = combine(groupby(items,
                                [:DATES, :ITEM]
                               ),
                        :ITEM => length => :COUNT
                       )
    transform!(groupby(max_items,
                       :DATES
                      ),
               :COUNT => maximum => :MAX_COUNT
              )
    result = sort(select(filter(row -> row.COUNT == row.MAX_COUNT,
                                max_items
                               ),
                         :DATES,
                         :ITEM
                        ),
                  :DATES
                 )
    println("\n$SPACE")
    println("Most frequented item by each date:\n$result")
end

#*/________________________________

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
else
    println("\n ARCHIVE LOADED AS MODULE, EXECUTE MAIN() MANUALLY WITH SQLITE AND ITEMS AS ARGS.")
end
