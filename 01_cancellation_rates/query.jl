url="https://raw.githubusercontent.com/Uriel1201/HelloSQL2.0/refs/heads/main/01_cancellation_rates/data.tsv";
download(url, "users.csv")

begin
    using Pkg
    Pkg.add(["CSV", "DataFrames", "StatsModels", "MLBase"])
    using CSV
    using DataFrames
    using StatsModels
    using MLBase
end

df=CSV.read("users.csv",DataFrame)
sample=first(df,5)
println(sample)

dummy=select(df, :USER_ID, [:ACTION => ByRow(isequal(v)) => Symbol(v) for v in unique(df.ACTION)])
totals=combine(groupby(dummy, :USER_ID), names(dummy, Not(:USER_ID)) .=> sum)
totals.cancel_rate = totals.cancel_sum ./ totals.start_sum
totals.publish_rate = totals.publish_sum ./ totals.start_sum
result = select(
    totals,
    :USER_ID,
    :cancel_rate,
    :publish_rate
)
println(result)
