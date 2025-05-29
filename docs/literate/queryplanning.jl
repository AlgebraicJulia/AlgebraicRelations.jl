using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

include("examples/wineries.jl");

incident(fabric, :GreenGrape, :species)

incident(fabric, [(:Green, :color), (:GreenGrape, :species)])

# TODO add the Join statement to track the first where
# q = From(:Winemaker) |> Where([:Winemaker, :country_code], ==, [:Country, :id]) |>
#                         Where([:Country, :country], ==, "France") |>
#                         Select(:Country!country)

# execute!(fabric, q)

# view_graphviz(to_graphviz(fabric.graph))

diag = @relation (winemaker_name=winemaker_name) begin
    WineWinemaker(wwm_wine=wine, wwm_winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker_name)
    CountryClimate(id=region, cc_country=country_id)
    Country(id=country_id, country=country)
    Wine(id=wine, cultivar=grape)
    Grape(id=grape, color=color, species=species)
end

view_graphviz(to_graphviz(diag, box_labels=:name, junction_labels=:variable))

view_graphviz(to_graphviz(diag, box_labels=true))

# look at the UWD in question
# view_graphviz(to_graphviz(get_port, box_labels=true, junction_labels=:variable))

# helpful to have the Schema visualized when doing lots of subpart/incident
view_graphviz(to_graphviz(Presentation(acset_schema(diag)), graph_attrs=Dict(:size=>"5", :ratio=>"expand")))

# q = QueryRopeGraph(diag)

query(fabric, diag)

# use valence to generate boxpaths
# get independent boxpaths by taking intersection
boxpath(diag, 6, 2)
boxpath(diag, 4, 2)

params = [JQParam(:_, :color, :Green), JQParam(:_, :species, :GreenGrape)]
# querying
map(boxpath(diag, 6, 2)) do (l, r)
    query_boxes(fabric, diag, l, r; params=params) 
    # TODO this result will be a param for the previous 
end

p1=query_boxes(fabric, diag, 6, 5; params=params)
p2=query_boxes(fabric, diag, 5, 1; params=p1)
p3=query_boxes(fabric, diag, 1, 2; params=p2)

function foo(params=(;))
    params
end

paths=boxpath(diag, 6, 2)

ids=foldl(paths; init=params) do param, path
    query_boxes(fabric, diag, path...; params=param)
end

params = JQParam(:_, :country, :Italy)
q1=query_boxes(fabric, diag, 4, 3; params=params)

params = JQParam(:_, :country, :Italy)
q1=query_boxes(fabric, diag, 4, 3; params=params)
q2=query_boxes(fabric, diag, 3, 2; params=q1)

# get winemakers
# TODO note that since Winemaker_id is a FK, it is an ID, so we don't need 
# to call incident on it
res = incident(fabric, q2.vals, :region)
## ANSWER ^^^

query(fabric, diag, (species=:GreenGrape, color=:Green, country=:Italy))

subpart(fabric, res._id ∩ p3.vals, :winemaker) 

incident(fabric, p3.vals, :Winemaker_id)

incident(fabric, FK{Grape}(1), :cultivar)


# incident on DB + InMemory (ACSet)
v1 = incident(fabric, :Graph; color=:Red, species=:RedGrape) 

v2 = incident(fabric, :Wine; grape=v1)
v3 = incident(fabric, 


# execute!(fabric,
# """
# from Winemaker w
# left join Country c
# on w.country_code = c.id
# where c.country = "France"
# select w.winemaker
# """)

using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram



function Base.empty(fabric::DataFabric)
    typ = typeof(fabric.graph)()
end

function Base.similar(fabric::DataFabric)
    out = Base.empty(fabric)
    @info out
    foreach(objects(acset_schema(out))) do ob
        add_parts!(out, ob, length(parts(out, ob)))
    end
    out
end

view_graphviz(to_graphviz(diagram))

# TODO r[:junction .== 1]

# - When a query involves a single join, its best to take the fiber product, or equivalently, the conditional (theta) join
# - When a query contains multiple joins, we can probably rewrite it to execute simpler queries independently. We rewrite the query


