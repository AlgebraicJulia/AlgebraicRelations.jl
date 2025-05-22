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

# returns HTML Document
function ACSetInterface.subpart(web::WebAPI; path::String="", kws...)
    response = HTTP.request("GET", web.conn, kws...)
    @assert response.status == 200
    parsed_doc = parsehtml(String(response.body))
    query(parsed_doc, path)
end

function ACSetInterface.incident(web::WebAPI, id, column::Union{Symbol, Nothing}=nothing)
    
end

# HTTP.get(...) # select # subpart
# HTTP.post(...) # insert
# HTTP.put(...) # upsert
# HTTP.delete(...)
# HTTP.patch(...) #
# HTTP.head(...)

