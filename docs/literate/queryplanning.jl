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

view_graphviz(to_graphviz(diag, box_labels=:name, junction_labels=:variable))

view_graphviz(to_graphviz(diag, box_labels=true))

# look at the UWD in question
view_graphviz(to_graphviz(get_port, box_labels=true, junction_labels=:variable))

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
# TODO throw bounds error
Base.getindex(iter::PairIterator, idx::Int64) = idx ≤ length(iter) ? Tuple(iter.data[idx:idx+1]) : error("bounds error")
Base.lastindex(iter::PairIterator) = length(iter)
# iter[i:j]

function boxpath(diag::UntypedNamedRelationDiagram, start::Int, stop::Int)
    path = Int[start]
    boxes = [start]
    while true
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

# use valence to generate boxpaths
# get independent boxpaths by taking intersection
boxpath(diag, 6, 2)
boxpath(diag, 4, 2)

incident(fabric, [(params[col], col) for col in [:color, :species]])


get_box=@relation (variable=variable) begin
    Junction(_id=Junction, variable=variable)
    Port(box=left, junction=Junction)
    Port(box=right, junction=Junction)
end

get_port=@relation (Port=Port, port_name=port_name) begin
    Port(_id=Port, box=box, junction=junction_id, port_name=port_name)
    Junction(_id=junction_id, variable=junction)
end

# query(diag, get_port, (box=5,junction=:grape))

struct JQParam
    junction::Symbol
    port_name::Symbol
    vals # could be ids
end

params = Dict(:color => :Green, :species => :GreenGrape)

params = [JQParam(:_, :color, :Green), JQParam(:_, :species, :GreenGrape)]

function query_boxes(fabric::DataFabric, diagram::UntypedNamedRelationDiagram, left::Int, right::Int; params::Union{JQParam, Vector{JQParam}}=JQParam[])
    # box
    box = diagram[left, :name]
    # params
    js = box_junctions(diagram, left)
    _params = js[arity.(Ref(diagram), js, Ref(:junction)) .== 1]
    param_names = subpart(diagram, _params, :variable)
    newport = setdiff(diagram[incident(diag, left, :box), :port_name], diagram[incident(diag, right, :box), :port_name]) 
    #
    _result = incident(fabric, [(jq.vals, jq.port_name) for jq in [JQParam[]; params]])
    # _result = incident(fabric, [(params[col], col) for col in param_names])
    # which junction mediates 6, 5
    junct, = js ∩ box_junctions(diagram, right)
    # port_id, = incident(diag, right, :box) ∩ incident(diag, junct, :junction)
    result = query(diagram, get_box, (left=left, right=right))
    junct_name = diagram[junct,:variable]
    port_name = query(diagram, get_port, (box=right,junction=diagram[junct,:variable])).port_name
    JQParam(only(result.variable), only(port_name), _result)
end

# querying
map(boxpath(diag, 6, 2)) do (l, r)
    query_boxes(fabric, diag, l, r; params=params) # TODO this result will be a param for the previous 
end

p1=query_boxes(fabric, diag, 6, 5; params=params)
p2=query_boxes(fabric, diag, 5, 1; params=p1)
p3=query_boxes(fabric, diag, 1, 2; params=p2)


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


