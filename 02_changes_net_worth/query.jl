begin
    using Pkg
    Pkg.add(["DataFrames", "SQLite", "Dates"])
    using DataFrames
    using SQLite
    using Dates
end

db = SQLite.DB()

#*/________________________________
schema = Tables.Schema((:SENDER, :RECEIVER, :AMOUNT, :TRANSACTION_DATE), (Int32, Int32, Float64, String))
SQLite.createtable!(db, "TRANSACTIONS", schema, temp = false)
rows = [(5, 2, 10.0, "12-feb-20"),
        (1, 3, 15.0, "13-feb-20"),
        (2, 1, 20.0, "13-feb-20"),
        (2, 3, 25.0, "14-feb-20"),
        (3, 1, 20.0, "15-feb-20"),
        (3, 2, 15.0, "15-feb-20"),
        (1, 4, 5.0, "16-feb-20")]

SQLite.execute(db, "BEGIN TRANSACTION")
placeholders = join(["(?, ?, ?, ?)" for _ in rows], ", ")
query = "INSERT INTO TRANSACTIONS (SENDER, RECEIVER, AMOUNT, TRANSACTION_DATE) VALUES $placeholders"
stmt = SQLite.Stmt(db, query)
params = collect(Iterators.flatten(rows))
DBInterface.execute(stmt, params)
SQLite.execute(db, "COMMIT")
#*/________________________________
transactions = DBInterface.execute(db, "SELECT * FROM TRANSACTIONS") |> DataFrame

space = "#*/________________________________"
sample = first(transactions, 5)
println("$space")
println("\n TRANSACTIONS TABLE -> SAMPLE:\n$sample")

type_ = (rename!(stack(select(sample,
                              :AMOUNT,
                              :SENDER,
                              :RECEIVER
                             ),
                       2:3
                      ),
                 :variable => :TYPE,
                 :value => :USER_ID
                )
)
transform!(type_, [:TYPE, :AMOUNT] => ByRow((x, y) -> x == "SENDER" ? -1 * y : y) => :AMOUNT)

println("\n$space")
println("\n Type of transaction made by each user -> SAMPLE:\n$type_")

df = (rename!(stack(select(transactions,
                           :AMOUNT,
                           :SENDER,
                           :RECEIVER
                          ),
                    2:3
                   ),
              :variable => :TYPE,
              :value => :USER_ID
             )
)
transform!(df, [:TYPE, :AMOUNT] => ByRow((x, y) -> x == "SENDER" ? -1 * y : y) => :AMOUNT)

result = (sort(combine(groupby(df,
                               :USER_ID
                              ),
                       :AMOUNT => sum
                      ),
               :AMOUNT_sum,
               rev = true
              )
)
print("\n$space")
println("\n Net changes:\n$result")
