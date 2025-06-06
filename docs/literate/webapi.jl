# # Web API-Backed ACSets
using Catlab
using ACSets
using AlgebraicRelations

axolt = "https://theaxolotlapi.netlify.app/"

resp = HTTP.request("GET", axolt)
resp = HTTP.get(axolt)
println(resp.status)
println(String(resp.body))

resp = HTTP.get("http://httpbin.org/anything"; query=["hello" => "world"])
