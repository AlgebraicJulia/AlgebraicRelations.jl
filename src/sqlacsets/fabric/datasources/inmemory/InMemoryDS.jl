module InMemoryDS

using ACSets

using DataFrames
using ..Fabric
import ..Fabric: recatalog!, columntypes

# this is an ACSet
mutable struct InMemory <: AbstractDataSource
    value
    # TODO need better method
    function InMemory(value::AbstractDataSource)
        error("No!")
    end
    function InMemory(value)
        new(value)
    end
end
export InMemory

Base.nameof(m::InMemory) = nameof(m.value)

Base.nameof(x::ACSet) = nameof(typeof(x))

Fabric.columntypes(m::InMemory) = columntypes(m.value)

function Fabric.columntypes(x::ACSet)
    schema = acset_schema(x)
    attrtype_mapping = Dict([col => type for (col, type) in zip(attrtypes(schema), [typeof(x).parameters...])])
    Dict([name => attrtype_mapping[attrtype] for (name, _, attrtype) in acset_schema(x).attrs]...)
end

function recatalog!(m::InMemory); m end
export recatalog!

function DenseACSets.acset_schema(m::InMemory)
    acset_schema(m.value)
end

function ACSetInterface.nparts(m::InMemory, args...)
    nparts(m.value, args...)
end

function ACSetInterface.add_part!(m::InMemory, args...)
    add_part!(m.value, args...)
end

function ACSetInterface.add_parts!(m::InMemory, args...)
    add_parts!(m.value, args...)
end

function ACSetInterface.subpart(m::InMemory, Colon, tablecolumn::Pair{Symbol, Symbol})
    df = DataFrame()
    result = subpart(m.value, :, tablecolumn.second)
    df[!, tablecolumn.second] = result isa AbstractVector ? result : [result]
    df
end

# TODO add types
function ACSetInterface.subpart(m::InMemory, id, column::Symbol)
    df = DataFrame()
    result = subpart(m.value, id, column)
    df[:, column] = result
end

function ACSetInterface.incident(m::InMemory, id, tablecolumn::Pair{Symbol, Symbol})
    incident(m.value, id, tablecolumn.second)
end

end
