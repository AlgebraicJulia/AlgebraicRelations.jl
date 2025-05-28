using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

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

# look at the UWD in question
to_graphviz(diag, box_labels=true, junction_labels=:variable)

# helpful to have the Schema visualized when doing lots of subpart/incident
to_graphviz(Presentation(acset_schema(diag)), graph_attrs=Dict(:size=>"5", :ratio=>"expand"))

# ------------------------------
# incident along multiple homs

# the type for `f` is a bit complex but it lets us handle (:f, (a:, :b)) to do multi-indexing
# on paths of different lengths for each leg in the (multi)spans
function incident_multi(acs, parts, f::T) where{T<:Tuple{Vararg{Union{Symbol, Tuple{Vararg{Symbol}}}}}}
    return intersect([incident(acs, parts[i], f[i]) for i in eachindex(f)]...)
end

@present SpanSch(FreeSchema) begin
    (X,Y,Z)::Ob
    Val::AttrType
    x::Hom(Z,X)
    y::Hom(Z,Y)
    val::Attr(X,Val)
end

to_graphviz(SpanSch, graph_attrs=Dict(:size=>"4", :ratio=>"expand"))

@acset_type SpanType(SpanSch, index=nameof.(generators(SpanSch, :Hom)))

span_acs = @acset SpanType{Symbol} begin
    Z=5
    Y=3
    X=3
    x=[1,2,2,3,3]
    val=[:a,:b,:b]
    y=[3,3,2,2,1]
end

# manually, and doing it with `incident_multi`, they return the same
intersect(
    incident(span_acs, 3, :y),
    incident(span_acs, :b, (:x, :val))
)

incident_multi(span_acs, (3, :b), (:y, (:x, :val)))

# end sean's section
# ------------------------------

q = QueryRopeGraph(diag)

function arity(diag::UntypedNamedRelationDiagram, j::Int)
    length(incident(diag, j, :junction))
end


function box_junctions(diag::UntypedNamedRelationDiagram, b::Int)
    diag[incident(diag, b, :box), :junction]
end

function neighboring_boxes(diag::UntypedNamedRelationDiagram, b::Int, path::Vector{Int})
    junctions_of_box_id = box_junctions(diag, b) # 7, 8, 9
    neighbor = setdiff(diag[vcat(incident(diag, junctions_of_box_id, :junction)...), :box], b)
    setdiff(neighbor, path)
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

map(boxpath(diag, 6, 2)) do (l, r)
    # box
    diag[l, :name]
    # params
    js = box_junctions(diag, l)
    params = js[arity.(Ref(diag), js) .== 1]
end

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


