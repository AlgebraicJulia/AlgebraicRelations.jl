module SQLACSets

using ACSets
using Catlab

using MLStyle
using FunSQL
using DataFrames
using DBInterface

using Reexport

# hand-rolled SQL syntax. necessary for DML operations
include("syntax.jl") 

# defines the Data Fabric concept
include("fabric/Fabric.jl")

include("methods.jl") # the VirtualACSet

@reexport using .SQLACSetSyntax
@reexport using .Fabric


end
