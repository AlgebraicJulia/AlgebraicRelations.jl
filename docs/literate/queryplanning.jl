using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface

include("examples/wineries.jl");

# TODO add the Join statement to track the first where
# q = From(:Winemaker) |> Where([:Winemaker, :country_code], ==, [:Country, :id]) |>
#                         Where([:Country, :country], ==, "France") |>
#                         Select(:Country!country)

# execute!(fabric, q)

# view_graphviz(to_graphviz(fabric.graph))

diag = @relation (winemaker_name=winemaker_name) begin
    WineWinemaker(wine=wine, winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker_name)
    CountryClimate(id=region, country=country_id)
    Country(id=country_id, country=name)
    Wine(id=wine, grape=grape)
    Grape(id=grape, color=color, species=species)
end

q = QueryRopeGraph(diag)

# execute!(fabric,
# """
# from Winemaker w
# left join Country c
# on w.country_code = c.id
# where c.country = "France"
# select w.winemaker
# """)

using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram


function Catlab.query(fabric::DataFabric, diagram::UntypedNamedRelatedDiagram, params=(;))
    rope = QueryRopeGraph(diagram)
end

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


