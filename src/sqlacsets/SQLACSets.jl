module SQLACSets

using ACSets

using MLStyle
using FunSQL
using DataFrames
using DBInterface

# query an ACSet with a SQL syntax
include("query.jl")
# query a table using the ACSets interface
include("syntax.jl")
include("methods.jl")
include("acsets_interface.jl")

end
