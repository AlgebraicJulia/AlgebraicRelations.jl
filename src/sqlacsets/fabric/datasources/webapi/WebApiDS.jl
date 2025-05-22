module WebAPIDS

using ..Fabric

using ACSets

using HTTP
using Gumbo # HTML Parsing

@kwdef struct WebAPI <: AbstractDataSource
    conn::String # HTTP endpoint
    log::Vector{Log} = Log[]
end
export WebAPI

# y = WebAPI(conn="https://theaxolotlapi.netlify.app/")

# resp = HTTP.request("GET", yy.conn)

# objects are endpoints
# attrs are query params

# TODO convert FunSQL into Query Parameters

function Fabric.reconnect!(::WebAPI) end

function Fabric.execute!(webapi::WebAPI, stmt::AbstractString; formatter=nothing)
end

end
