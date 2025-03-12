module InMemoryDS

using ACSets
using ..Fabric
import ..Fabric: recatalog!

# this is an ACSet
mutable struct InMemory <: AbstractDataSource
    value
    function InMemory(value::AbstractDataSource)
        error("No!")
    end
    function InMemory(value)
        new(value)
    end
end
export InMemory

function recatalog!(m::InMemory); m end
export recatalog!

function ACSetInterface.add_parts!(m::InMemory, args...; kwargs...)
    add_parts!(m.value, args...; kwargs...)
end
export add_parts!

function ACSetInterface.subpart(m::InMemory, (:), tablecolumn::Pair{Symbol, Symbol})
    result = subpart(m.value, :, tablecolumn.second)
    DataFrame(NamedTuple{(tablecolumn.second,)}(Tuple([result])))
end
export subpart

function ACSetInterface.subpart(m::InMemory, id, column::Symbol)
    subpart(m.value, id, column)
end

function ACSetInterface.incident(m::InMemory, id, tablecolumn::Pair{Symbol, Symbol})
    incident(m.value, id, tablecolumn.second)
end
export incident

end
