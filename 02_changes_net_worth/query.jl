begin
    using Pkg
    Pkg.add(["CSV", "DataFrames"])
    using CSV
    using DataFrames
end

url = "https://raw.githubusercontent.com/Uriel1201/HelloSQL2.0/refs/heads/main/02_changes_net_worth/data.tsv";
download(url, "transactions.tsv")
transactions = CSV.read("transactions.tsv", DataFrame; delim = '\t')

sample = first(transactions, 5)
println("transactions table (SAMPLE):\n$sample")

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
println("\nType of transaction made by each user (SAMPLE):\n$type_")

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
println("\nNet changes:\n$result")
