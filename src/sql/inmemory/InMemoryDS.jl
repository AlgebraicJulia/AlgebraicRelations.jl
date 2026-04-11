module InMemoryDS

using ACSets
using TraitInterfaces
using DataFrames

using ..SQL: ThDataSource, AbstractDataSource, Encoded
import ..SQL: columntypes, encode_attr
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
    schema(m::InMemory) = DenseACSets.acset_schema(value)
end

Base.nameof(m::InMemory) = nameof(typeof(m.value))

# TODO migrate to ACSets
function columntypes(x::ACSet)
    schema = acset_schema(x)
    attrtype_mapping = Dict([col => type for (col, type) in zip(attrtypes(schema), [typeof(x).parameters...])])
    Dict([name => attrtype_mapping[attrtype] for (name, _, attrtype) in acset_schema(x).attrs]...)
end
columntypes(m::InMemory) = columntypes(m.value)


encode_attr(m::InMemory) = encode_attr(m.value)

# TODO dispatch on generator
function encode_attr(acset::ACSet)
    s = acset_schema(acset)
    as = attrs(s)
    obs = objects(s)
    ob_attrs = Dict(ob => first.(getindex(as, findall(x->x[2]==ob, as))) for ob in obs)
    Dict(ob => Dict(attr => encode_attr(acset, attr) for attr in ob_attrs[ob]) for ob in obs) 
end
export encode_attr

function encode_attr(acset::ACSet, attr::Symbol)
    vals = acset[attr]
    uniq = unique(vals)
    lookup = Dict(v => i for (i, v) in enumerate(uniq))
    encoded = [lookup[v] for v in vals]
    return Encoded(length(uniq), encoded, uniq)
end


include("acset_interface.jl")

end
