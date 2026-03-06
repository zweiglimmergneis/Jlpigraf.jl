module Jlpigraf


using HTTP, URIs
using JSON3, CSV
using DataFrames

export api_setup, api_fetch, fetch_table

include("api.jl")
include("fetch.jl")

end
