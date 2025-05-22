begin
    using Pkg
    Pkg.add(["CSV", "DataFrames", "Dates", "ShiftedArrays"])
    using CSV
    using DataFrames
    using Dates
    using ShiftedArrays
end

url = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/04_time_difference/data.tsv"
download(url, "users.tsv")
users = CSV.read("users.tsv", DataFrame; delim = '\t')

users[!, :ACTION_DATE] = Date.(split.(users.ACTION_DATE, "T") .|> first)
sample = sort(first(users,
                    5
                   ),
              [:ID, :ACTION_DATE],
              rev = [false,true]
             )
println("USERS TABLE (SAMPLE -> 5):\n$sample")

transform!(groupby(sample, 
                   :ID
                  ),
           :ACTION_DATE =>
           (x -> (x .- ShiftedArrays.lead(x, 1))
           ) => :ELAPSED_DAYS
          )
println("\nDIFFERENCE BETWEEN CONSECUTIVE ACTIONS (SAMPLE -> 5):\n$sample")

users = sort(users,
             [:ID, :ACTION_DATE],
             rev = [false,true]
            )

transform!(groupby(users, 
                   :ID
                  ),
           :ACTION_DATE =>
           (x -> (x .- ShiftedArrays.lead(x, 1))
           ) => :ELAPSED_DAYS
          )

result = select(combine(groupby(users, 
                                :ID
                               ), 
                        :ELAPSED_DAYS => first => :ELAPSED_DAYS
                       ), 
                :ID, 
                :ELAPSED_DAYS
               )
result.ELAPSED_DAYS = passmissing(x -> x.value).(result.ELAPSED_DAYS)

println("\nRETURNING ELAPSED TIME BETWEEN THE TWO LAST ACTIVITIES:\n$result")
