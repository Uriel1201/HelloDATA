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
sort!(sample,
      [:USER_ID, :TRANSACTION_DATE]
     )
println("\n USERS TABLE -> SAMPLE:\n$sample")

transform!(groupby(sample,
                   :USER_ID
                  ),
           :TRANSACTION_DATE => (x -> ShiftedArrays.lead(x,
                                                         1
                                                        )
                               ) => :SUPERDATE
          )
println("\n IDENTIFYING DATES AS SUPER -> SAMPLE:\n$sample")

sort!(users,
      [:USER_ID, :TRANSACTION_DATE]
     )
transform!(groupby(users,
                   :USER_ID
                  ),
           :TRANSACTION_DATE => (x -> ShiftedArrays.lead(x,
                                                         1
                                                        )
                                ) => :SUPERDATE
          )
result = combine(groupby(users,
                         :USER_ID
                        ),
                 :SUPERDATE => first => :SUPERDATE
                )
println("\n RETURNING DATE WHEN USER GOT SUPERUSER MEMBERSHIP:\n$result")
