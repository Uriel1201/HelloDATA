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
println("$users")
"""
dummy = select(users, :USER_ID, [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(users.ACTION)])
println("\n$dummy")
totals = combine(groupby(dummy, :USER_ID), names(dummy, Not(:USER_ID)) .=> sum)
totals.cancel_rate = @.ifelse(totals.start_sum != 0, totals.cancel_sum ./ totals.start_sum, 0.0)
totals.publish_rate = @.ifelse(totals.start_sum != 0, totals.publish_sum ./ totals.start_sum, 0.0)
result = select(
    totals,
    :USER_ID,
    :cancel_rate,
    :publish_rate
)
println("\n$result")
