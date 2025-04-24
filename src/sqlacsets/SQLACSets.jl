module SQLACSets

using ACSets
using Catlab

using MLStyle
using FunSQL
using DataFrames
using DBInterface

using Reexport

# hand-rolled SQL syntax. necessary for DML operations, since FunSQL does not provide that
include("syntax.jl") 

# defines the Data Fabric concept
include("fabric/Fabric.jl")

# the VirtualACSet
include("methods.jl")

@reexport using .SQLACSetSyntax
@reexport using .Fabric


end
