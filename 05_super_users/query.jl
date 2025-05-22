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


sample = first(users, 
               5
              )
println("\n")
