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
    WineWinemaker(wine=wine, winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker_name)
    CountryClimate(id=region, country=country_id)
    Country(id=country_id, country=name)
    Wine(id=wine, cultivar=grape)
    Grape(id=grape, color=color, species=species)
end

# look at the UWD in question
view_graphviz(to_graphviz(diag, box_labels=true, junction_labels=:variable))

# helpful to have the Schema visualized when doing lots of subpart/incident
view_graphviz(to_graphviz(Presentation(acset_schema(diag)), graph_attrs=Dict(:size=>"5", :ratio=>"expand")))

# q = QueryRopeGraph(diag)

function arity(diag::UntypedNamedRelationDiagram, i::Int, label::Symbol=:junction)
    length(incident(diag, i, label))
end

function box_junctions(diag::UntypedNamedRelationDiagram, b::Int)
    diag[incident(diag, b, :box), :junction]
end

function neighboring_boxes(diag::UntypedNamedRelationDiagram, b::Int, path::Vector{Int}=Int[])
    junctions_of_box_id = box_junctions(diag, b) # 7, 8, 9
    neighbor = setdiff(diag[vcat(incident(diag, junctions_of_box_id, :junction)...), :box], b)
    setdiff(neighbor, path)
end

# this should in actuality check neighboring boxes
function valence(diag::UntypedNamedRelationDiagram)
    map(parts(diag, :Box)) do box
        box => (arity=arity(diag, box, :box), neighbors=neighboring_boxes(diag, box))
    end
end

struct PairIterator{T}; data::Vector{T} end
function Base.iterate(iter::PairIterator, state::Int=1)
    if state < length(iter.data)
        ((iter.data[state], iter.data[state+1]), state+1) 
    end
end
Base.IteratorSize(::Type{PairIterator}) = Base.HasLength()
Base.length(iter::PairIterator) = max(0, length(iter.data) - 1)

function boxpath(diag::UntypedNamedRelationDiagram, start::Int, stop::Int)
    keep_on = true
    path = Int[start]
    boxes = [start]
    while keep_on
        isempty(boxes) && break
        res, = neighboring_boxes.(Ref(diag), boxes, Ref(path))
        union!(path, res)
        boxes = res
        if stop ∈ res
            break
        end
    end
    PairIterator(path)
end

params = Dict(:color => :Green, :species => :GreenGrape)

incident(fabric, [(params[col], col) for col in [:color, :species]])


get_box=@relation (variable=variable) begin
    Junction(_id=Junction, variable=variable)
    Port(box=left, junction=Junction)
    Port(box=right, junction=Junction)
end

function query_boxes(fabric::DataFabric, diagram::UntypedNamedRelationDiagram, left::Int, right::Int)
    # box
    box = diag[left, :name]
    # params
    js = box_junctions(diag, left)
    _params = js[arity.(Ref(diag), js, Ref(:junction)) .== 1]
    param_names = subpart(diag, _params, :variable)
    #
    result = incident(fabric, [(params[col], col) for col in param_names])
    # which junction mediates 6, 5
    _res = query(diag, get_box, (left=left, right=right))
    _res.variable => result
end

# querying
map(boxpath(diag, 6, 2)) do (l, r)
    query_boxes(fabric, diag, l, r) # TODO this result will be a param for the previous 
end

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


