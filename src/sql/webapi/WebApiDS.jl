module WebAPIDS

using ..Fabric

using ACSets

using HTTP
using Gumbo # HTML Parsing

struct Field
    name::Symbol
end
export Field

(Base.:!)(x::Field) = Base.Fix2(getproperty, x.name)

@kwdef mutable struct WebAPIConnection
    const value::Union{String, Function}
    const kvs::Vector{Symbol} = []
    endpoint::Union{String, Nothing}=nothing
end
export WebAPIConnection

endpoint(wac::WebAPIConnection) = wac.endpoint

function connect(string::String)
    matches = eachmatch(r"{{([a-z]*)}}", string)
    kwargs = [Symbol(m.captures[1]) for m in matches]
    out = (; kws...) -> begin
        ks = keys(kws)
        if !isempty(matches)
            if kwargs != [ks...]
                error("$kwargs does not equal $(ks...))")
            end
            replace(string, ["{{$k}}" => v for (k,v) in kws]...)
        else
            string
        end
    end
    WebAPIConnection(value=out, kvs=kwargs, endpoint=string)
end
export connect

@kwdef mutable struct WebAPI <: AbstractDataSource
    const conn::WebAPIConnection # HTTP endpoint
    paths::Dict{Symbol, String} = Dict{Symbol, String}()
    token_envar::Union{String, Nothing} = nothing
    log::Vector{Log} = Log[] 
end
export WebAPI

function WebAPI(conn::String; kwargs...)
    WebAPI(; conn=connect(conn), kwargs...)
end

function Base.show(io::IO, web::WebAPI)
    print(io, "Arity $(join(web.conn.kvs, ", "))")
end

endpoint(web::WebAPI) = endpoint(web.conn)

function add_path!(web::WebAPI, path::Pair{Symbol, String})
    web.paths[path.first] = path.second
end
export add_path!

function Base.getindex(web::WebAPI, args...; kwargs...)
    web.conn.endpoint = web.conn.value(;kwargs...) * "$(args...)"
    @info "Endpoint set to:
    
        $(web.conn.endpoint)
    "
    web
end

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
