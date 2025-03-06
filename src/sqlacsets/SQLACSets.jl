module SQLACSets

using ACSets
using Catlab

using MLStyle
using FunSQL
using DataFrames
using DBInterface

# hand-rolled SQL syntax. necessary for DML operations
include("syntax.jl") 
include("methods.jl") # the VirtualACSet
include("acsets_interface.jl") # ACSetInterface

end
