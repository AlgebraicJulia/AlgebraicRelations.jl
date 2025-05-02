module WebAPIDS

using ..Fabric

using ACSets

using HTTP

@kwdef struct WebAPI <: AbstractDataSource
    conn::String # HTTP endpoint
    log::Vector{Log} = Log[]
end
export WebAPISource

# objects are endpoints
# attrs are query params

# TODO convert FunSQL into Query Parameters

function Fabric.recatalog!(::WebAPI) end

function Fabric.execute!(webapi::WebAPI, stmt::AbstractString; formatter=nothing)
end

function ACSetInterface.subpart(web::WebAPI, column::Symbol)
    HTTP.get(web.conn; query=[column => column])
end

function ACSetInterface.incident(web::WebAPI, id, column::Symbol)
end

# HTTP.get(...) # select # subpart
# HTTP.post(...) # insert
# HTTP.put(...) # upsert
# HTTP.delete(...)
# HTTP.patch(...) #
# HTTP.head(...)

end
