module WebAPIDS

using ..Fabric

using ACSets

using HTTP
using Gumbo # HTML Parsing

@kwdef struct WebAPI <: AbstractDataSource
    conn::String # HTTP endpoint
    token_envar::Union{String, Nothing} = nothing # TODO best way to store secrets?
    log::Vector{Log} = Log[]
end
export WebAPI

function build_headers(web::WebAPI)
    if !isnothing(web.token_envar)
        Dict("authorization" => "Bearer $(ENV[web.token_envar])", 
             "accept" => "application/json;odata=verbse")
    end
end

# objects are endpoints
# attrs are query params

# TODO convert FunSQL into Query Parameters

function Fabric.reconnect!(::WebAPI) end

function Fabric.execute!(webapi::WebAPI, stmt::AbstractString; formatter=nothing)
end

include("acset_interface.jl")

end
