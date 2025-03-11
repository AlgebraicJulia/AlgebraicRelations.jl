module InMemory

using ACSets
using ..Fabric
import ..Fabric: recatalog!

function recatalog!(m::InMemory); m end
export recatalog!

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
