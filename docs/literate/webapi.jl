using AlgebraicRelations

# access
axolotl = WebAPI("https://theaxolotlapi.netlify.app/")

# we may specify where to get information on the webpage after we have defined `axolotl`.
add_path!(axolotl, :children=>"*.2.2.1.2.1.4.:children")

# now the `subpart` interface is like ACSet interface. We demonstrate that we can pass in the formatter function as a 
subpart(axolotl, :children; formatter=x->(first(x) |> Base.Fix2(getproperty, :text)))

# # Second part

# Let's specify a connection to the "genrenator" API
randomgenres = WebAPI("https://binaryjazz.us/wp-json/genrenator/v1/genre/")

# we instruct the webapi connection
add_path!(randomgenres, :text=>"*.2.1.:text")

# now the `subpart` interface is like the ACSet interface
subpart(randomgenres, :text)

using JSON3, Gumbo, HTTP

sOMOP = "cms_synthetic_patient_data_omop"

tableconn = WebAPI("https://redivis.com/api/v1/{{type}}/Demo.{{dataset}}"; token_envar="JULIA_OMOP_API_KEY")

# metadata = subpart(tableconn[dataset=sOMOP]) 

add_path!(tableconn, :text=>"*.2.1.:text")

subpart(tableconn[type="datasets", dataset=sOMOP, "/tables/"], :text) |> JSON3.read |> !Field(:results)

table = "care_site"

# cacheing queries.
# need an optional keyword
json_out = subpart(tableconn[type="tables", dataset=sOMOP, ".$table/rows?format=jsonl"], :text) |> Base.Fix2(split, "\n") .|> JSON3.read

using DataFrames
DataFrame(json_out) # because JSON3.Object support AbstractDict interface, yes it really is this easy

fabric = DataFabric()
