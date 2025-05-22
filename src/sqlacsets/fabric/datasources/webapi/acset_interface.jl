using ACSets

# returns HTML Document
function ACSetInterface.subpart(web::WebAPI, column::Union{Symbol, Nothing}=nothing)
    response = HTTP.request("GET", web.conn)
    parsehtml(String(response.body))
end

function ACSetInterface.incident(web::WebAPI, id, column::Union{Symbol, Nothing}=nothing)
    
end

# HTTP.get(...) # select # subpart
# HTTP.post(...) # insert
# HTTP.put(...) # upsert
# HTTP.delete(...)
# HTTP.patch(...) #
# HTTP.head(...)

