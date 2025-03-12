# get == select == subpart
#
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

const axolt = "https://theaxolotlapi.netlify.app/"

resp = HTTP.request("GET", axolt)
resp = HTTP.get(axolt)
println(resp.status)
println(String(resp.body))

resp = HTTP.get("http://httpbin.org/anything"; query=["hello" => "world"])
# TODO convert FunSQL into Query Parameters

function Fabric.recatalog!(::WebAPI) end

function Fabric.execute!(webapi::WebAPI, stmt::AbstractString; formatter=nothing)
end

function ACSetInterface.subpart(web::WebAPI, column::Symbol)
    HTTP.get(web.conn; query=[column => column])
end

function ACSetInterface.incident(web::WebAPI, id, column::Symbol)
end

# HTTP.get(...) # select
# HTTP.post(...) # insert
# HTTP.put(...) # upsert
# HTTP.delete(...)
# HTTP.patch(...) #
# HTTP.head(...)

end
