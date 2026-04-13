module InMemoryDS

using ACSets
using TraitInterfaces
using DataFrames

using ..SQL: ThDataSource, AbstractDataSource
import ..SQL: columntypes
import ...AlgebraicRelations: trait

# this is an ACSet
mutable struct InMemory <: AbstractDataSource
    value # maybe type by some wrapper for tabulated data
    # TODO need better method
    function InMemory(value::AbstractDataSource)
        error("No!")
    end
    function InMemory(value)
        new(value)
    end
end
export InMemory

struct InMemoryTrait end
trait(::InMemory) = InMemoryTrait()

TraitInterfaces.@instance ThDataSource{Source=InMemory,Statement=AbstractString} [model::InMemoryTrait] begin 
    reconnect!(m::InMemory)::InMemory = m
    execute!(m::InMemory, stmt::AbstractString)::Vector{Int} = Int[]
    schema(m::InMemory) = DenseACSets.acset_schema(m.value)
end

Base.nameof(m::InMemory) = nameof(typeof(m.value))

# TODO migrate to ACSets
function columntypes(x::ACSet)
    schema = acset_schema(x)
    attrtype_mapping = Dict([col => type for (col, type) in zip(attrtypes(schema), [typeof(x).parameters...])])
    Dict([name => attrtype_mapping[attrtype] for (name, _, attrtype) in acset_schema(x).attrs]...)
end
columntypes(m::InMemory) = columntypes(m.value)

include("acset_interface.jl")

end
