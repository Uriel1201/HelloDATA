begin
    using Pkg
    Pkg.add(["CSV", "DataFrames", "Dates", "ShiftedArrays"])
    using CSV
    using DataFrames
    using Dates
    using ShiftedArrays
end

url = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/05_super_users/data.tsv"
download(url, "users.tsv")
users = CSV.read("users.tsv", DataFrame; delim = '\t')
users[!, :TRANSACTION_DATE] = Date.(split.(users.TRANSACTION_DATE, "T") .|> first)

sample = first(users, 
               5
              )
println("\nUSERS TABLE -> SAMPLE:\n$sample")

a = groupby(sort(sample,
                 :USER_ID,
                 :TRANSACTION_DATE
                ),
            :USER_ID
           )
transform!(a,
           :TRANSACTIONDATE => (x -> ShiftedArrays.lead(x,
                                                        1
                                                       )
                               ) => :SUPERDATE
          )
