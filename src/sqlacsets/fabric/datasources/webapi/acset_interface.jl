using ACSets

using MLStyle

struct Accessors
    accessors
end

@active Re{r :: Regex}(x) begin
    res = match(r, x)
    if res !== nothing
        Some(res)
    else
        nothing
    end
end

function Accessors(path::String)
    accessors = []
    foreach(split(path, ".")) do a
        @match a begin
            Re{r"\d+"}(x) => begin
                push!(accessors, Base.Fix2(getproperty, :children))
                push!(accessors, Base.Fix2(getindex, parse(Int, a)))
            end
            Re{r":(\w+)"}(x) => begin
                push!(accessors, Base.Fix2(getproperty, Symbol(x.captures[1])))
            end
            "*" => begin
                push!(accessors, Base.Fix2(getproperty, :root))
            end
            _ => identity
        end
    end
    Accessors(accessors)
end

function query(doc::HTMLDocument, t::Accessors)
    foldl(|>, t.accessors; init=doc)
end
export query

function query(doc::HTMLDocument, path::String)
    query(doc, Accessors(path))
end

# its generally better to explicitly format the return value of the query, such as piping it through split+JSON3.read, but it may also be the case that we want a general format for how return data should be cached. 
#
function ACSetInterface.subpart(web::WebAPI, kws...; path::String="", formatter=identity)
    headers = build_headers(web)
    response = HTTP.request("GET", web.conn, headers, kws...)
    @assert response.status == 200
    parsed_doc = parsehtml(String(response.body))
    query(parsed_doc, path) |> formatter
end

function ACSetInterface.incident(web::WebAPI, id, column::Union{Symbol, Nothing}=nothing)
    
end

# HTTP.get(...) # select # subpart
# HTTP.post(...) # insert
# HTTP.put(...) # upsert
