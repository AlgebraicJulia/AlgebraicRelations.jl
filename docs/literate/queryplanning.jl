using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface

include("examples/wineries.jl")

# TODO add the Join statement to track the first where
q = From(:Winemaker) |> Where([:Winemaker, :country_code], ==, [:Country, :id]) |>
                        Where([:Country, :country], ==, "France") |>
                        Select(:Country!country)

execute!(fabric, q)

view_graphviz(to_graphviz(fabric.graph))

# execute!(fabric,
# """
# from Winemaker w
# left join Country c
# on w.country_code = c.id
# where c.country = "France"
# select w.winemaker
# """)

diagram = @relation (country_id=country_id) begin
    Winemaker(country_code=country_id)
    Country(id=country_id, country=country, climate=climate)
    Grape(species=species, country=country)
end

view_graphviz(to_graphviz(diagram))

# TODO r[:junction .== 1]

# - When a query involves a single join, its best to take the fiber product, or equivalently, the conditional (theta) join
# - When a query contains multiple joins, we can probably rewrite it to execute simpler queries independently. We rewrite the query


