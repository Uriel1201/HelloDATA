begin
    using Pkg
    Pkg.add(["CSV", "DataFrames"])
    using CSV
    using DataFrames
end

url = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/03_frequent_item/data.tsv";
download(url, "items.tsv")
items = CSV.read("items.tsv", DataFrame; delim = '\t')

sample = first(items, 5)
println(" Items table (SAMPLE = 5):\n$sample")

df = combine(groupby(sample,
                     [:DATES, :ITEM]
             ),
             :ITEM => length => :COUNT
     )
println("\nNumber of items by date (SAMPLE):\n$df")

max_items = combine(groupby(items,
                            [:DATES, :ITEM]
                           ),
                    :ITEM => length => :COUNT
                   )
transform!(groupby(max_items,
                   :DATES
                  ),
           :COUNT => maximum => :MAX_COUNT
          )
result = sort(select(filter(row -> row.COUNT == row.MAX_COUNT,
                            max_items
                           ),
                     :DATES,
                     :ITEM
                    ),
              :DATES
             )
println("\nMost frequented item by each date:\n$result")
