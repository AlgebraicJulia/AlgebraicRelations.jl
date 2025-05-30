using AlgebraicRelations

axolotl = WebAPI(conn="https://theaxolotlapi.netlify.app/")

subpart(axolotl)
subpart(axolotl; path="*.2.2.1.2.1.4.:children")
# subpart(axolotl; path="*.1.5.:children") 

randomgenres = WebAPI(conn="https://binaryjazz.us/wp-json/genrenator/v1/genre/")

subpart(randomgenres; path="*.2.1.:text")

# TODO store in WebAPI DS
access_token = ENV["JULIA_OMOP_API_KEY"];

using JSON3, Gumbo, HTTP

metadata = HTTP.request("GET", "https://redivis.com/api/v1/datasets/Demo.cms_synthetic_patient_data_omop", Dict("authorization" => "Bearer $access_token", "accept" => "application/json;odata=verbse",))

# list of "objects"

tableconn = WebAPI(conn="https://redivis.com/api/v1/datasets/Demo.cms_synthetic_patient_data_omop/tables", token_envar="JULIA_OMOP_API_KEY")

tables = subpart(tableconn; path="*.2.1.:text", formatter=JSON3.read)

table = "care_site"

queryconn = WebAPI(conn="https://redivis.com/api/v1/tables/Demo.cms_synthetic_patient_data_omop.$table/rows?format=jsonl", token_envar="JULIA_OMOP_API_KEY")

# cacheing queries. if 
json_out = subpart(queryconn; path="*.2.1.:text") |> Base.Fix2(split, "\n") .|> JSON3.read
using DataFrames
DataFrame(json_out) # because JSON3.Object support AbstractDict interface, yes it really is this easy

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

using Catlab
using ACSets

p=@present SchTables(FreeSchema) begin
    Value::AttrType
    Table::Ob
    (kind, id, qualifiedReference, scopedReference, referenceId, uri, url, isSample, hash, isFileIndex, createdAt, updatedAt, description, numRows, numBytes, variableCount, uploadMergeStrategy)::Attr(Table, Value)
end
@acset_type _Table(SchTables)
tables_acset = _Table{String}()
