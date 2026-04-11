DenseACSets.acset_schema(m::InMemory) = acset_schema(m.value)

function ACSetInterface.parts(m::InMemory, args...)
    parts(m.value, args...)
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

function ACSetInterface.subpart(m::InMemory, id, tablecolumn::Pair{Symbol, Symbol})
    df = DataFrame()
    result = subpart(m.value, id, tablecolumn.second)
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

function ACSetInterface.incident(m::InMemory, id, column::Symbol; formatter=identity)
    out = incident(m.value, id, column)
    formatter(out)
end

function ACSetInterface.incident(m::InMemory, parts, f::T; formatter=identity) where {T<:Tuple{Vararg{Union{Symbol, Tuple{Vararg{Symbol}}}}}}
    out = intersect([incident(m, parts[i], f[i]) for i in eachindex(f)]...)
    formatter(out)
end


