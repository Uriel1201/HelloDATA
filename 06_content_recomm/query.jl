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

s_friends = first(friends, 5)
s_likes = first(likes, 5)
println("\nFRIENDS TABLE (SAMPLE):\n$s_friends")
println("\nLIKES TABLE (SAMPLE):\n$s_likes")

s_jl = rename!(innerjoin(s_friends, 
                         rename(s_likes, 
                                :USER_ID => :FRIEND
                               ), 
                         on = :FRIEND
                        ),
               :PAGE_LIKES => :POSSIBLE
              )
println("\n IDENTIFYING POTENTIAL RECOMMENDATIONS (SAMPLE):\n$s_jl")

jl = select(innerjoin(friends, 
                      rename(likes, 
                             :USER_ID => :FRIEND
                            ), 
                      on = :FRIEND
                     ),
            :USER_ID,
            :PAGE_LIKES
           )
recommendations = antijoin(jl, 
                           likes, 
                           on = [:USER_ID, :PAGE_LIKES]
                          )
rename!(unique!(recommendations),
        :PAGE_LIKES => :RECOMMENDATIONS
       )
println("\n RECOMMENDATIONS:\n$recommendations")
