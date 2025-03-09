module WebAPI

using ..Fabric

using ACSets

using HTTP

@kwdef struct WebAPISource <: AbstractDataSource
    conn
    log::Vector{Log} = Log[]
end
export WebAPISource

function Fabric.recatalog!(::WebAPISource) end

function Fabric.execute!(webapi::WebAPISource, stmt::AbstractString; formatter=nothing)
end

function ACSetInterface.subpart end

end
