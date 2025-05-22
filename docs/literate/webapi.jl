using AlgebraicRelations


axolotl = WebAPI(conn="https://theaxolotlapi.netlify.app/")

subpart(axolotl)

randomgenres = WebAPI(conn="https://binaryjazz.us/wp-json/genrenator/v1/genre/")

subpart(randomgenres; path="*.2.1.:text")

# TODO store in WebAPI DS
access_token = ENV["JULIA_OMOP_API_KEY"]

using JSON3, Gumbo, HTTP

metadata = HTTP.request("GET", "https://redivis.com/api/v1/datasets/Demo.cms_synthetic_patient_data_omop", Dict("authorization" => "Bearer $access_token", "accept" => "application/json;odata=verbse",))

# list of "objects"
tables = HTTP.request("GET", "https://redivis.com/api/v1/datasets/Demo.cms_synthetic_patient_data_omop/tables", Dict("authorization" => "Bearer $access_token", "accept" => "application/json;odata=verbse",))

parsed_tables = parsehtml(String(tables.body))
JSON3.read(parsed_tables.root.children[2].children[1].text)

table = "care_site"
query = HTTP.request("GET", "https://redivis.com/api/v1/tables/Demo.cms_synthetic_patient_data_omop.$table/rows?format=jsonl", Dict(
    "authorization" => "Bearer $(access_token)",
    "accept" => "application/json;odata=verbose",
  ))

@assert query.status == 200

data = parsehtml(String(query.body))

rows = data.root.children[2].children

splitrows = split(rows[1].text, "\n")
parsed = JSON3.read.(splitrows)

struct Infer end

algrel_typeof(x) = typeof(something(x, Infer()))

function make_ob(obs::Vector{Symbol}, x::String)
    if x[end-2:end] == "_id"
        ob = join(uppercasefirst.(split(x[1:end-3], "_")))
        last(push!(obs, Symbol(ob)))
    else
        obs[1]
    end
end

using OrderedCollections

function guess_jsonob(j::JSON3.Object)
    obs = [:CareSite]
    OrderedDict([k => (make_ob(obs, String(k)), algrel_typeof(v)) for (k,v) in pairs(j)])
end

# guess the schema
# function guess()
    ats = algrel_typeof.(values(parsed[1]))
    BasicSchema([gensym()], [], [ats], [attrs], [])
# end
