begin
    using Pkg
    Pkg.add(["DataFrames", "SQLite", "Dates"])
    using DataFrames
    using SQLite
    using Dates
end

"""
Alternative 2: Querying directly from this repository 
users = CSV.read("users.tsv", DataFrame; delim = '\t')
"""

db = SQLite.DB()

#*/________________________________
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
#*/________________________________

users = DBInterface.execute(db, "SELECT * FROM USERS") |> DataFrame
sample = first(users, 5)
println("\n USERS TABLE -> SAMPLE:\n$sample")

dummy = select(users, :USER_ID, [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(users.ACTION)])
println("\n WAS THIS ACTION DONE BY USER -> SAMPLE:\n$dummy")
totals = combine(groupby(dummy, :USER_ID), names(dummy, Not(:USER_ID)) .=> sum)
totals.cancel_rate = @.ifelse(totals.start_sum != 0, totals.cancel_sum ./ totals.start_sum, 0.0)
totals.publish_rate = @.ifelse(totals.start_sum != 0, totals.publish_sum ./ totals.start_sum, 0.0)
result = select(
    totals,
    :USER_ID,
    :cancel_rate,
    :publish_rate
)
println("\nUSER STATISTICS:\n$result")
