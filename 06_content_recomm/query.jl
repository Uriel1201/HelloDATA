begin
    using Pkg
    Pkg.add(["CSV", "DataFrames"])
    using CSV
    using DataFrames
end

url_1 = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/06_content_recomm/data_friends.tsv"
url_2 = "https://raw.githubusercontent.com/Uriel1201/HelloDATA/refs/heads/main/06_content_recomm/data_likes.tsv"
download(url_1, "friends.tsv")
download(url_2, "likes.tsv")
friends = CSV.read("friends.tsv", DataFrame; delim = '\t')
likes = CSV.read("likes.tsv", DataFrame; delim = '\t')
