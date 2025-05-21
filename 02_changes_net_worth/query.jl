url = "https://raw.githubusercontent.com/Uriel1201/HelloSQL2.0/refs/heads/main/01_cancellation_rates/data.tsv";
download(url, "users.tsv")

begin
    using Pkg
    Pkg.add(["CSV", "DataFrames"])
    using CSV
    using DataFrames
end

users = CSV.read("users.tsv", DataFrame; delim = '\t')
println("$users")
