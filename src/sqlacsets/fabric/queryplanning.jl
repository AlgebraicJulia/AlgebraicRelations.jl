using ACSets
using ACSets.Query
import ACSets.Query: WhereCondition, AndWhere, OrWhere
using Catlab
using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

using OrderedCollections: OrderedDict
using MLStyle: @match, @λ
using StructEquality

struct PairIterator{T}
    data::Vector{T} 
end

function Base.iterate(iter::PairIterator, state::Int=1)
    if state < length(iter.data)
        ((iter.data[state], iter.data[state+1]), state+1) 
    end
end
Base.IteratorSize(::Type{PairIterator}) = Base.HasLength()
Base.length(iter::PairIterator) = max(0, length(iter.data) - 1)
function Base.getindex(iter::PairIterator, idx::Int64)
    idx ≤ length(iter) ? Tuple(iter.data[idx:idx+1]) : throw(BoundsError(length(iter), idx))
end
Base.lastindex(iter::PairIterator) = length(iter)
# iter[i:j]

function arity(diag::UntypedNamedRelationDiagram, i::Int, label::Symbol=:junction)
    length(incident(diag, i, label))
end
export arity

function box_junctions(diag::UntypedNamedRelationDiagram, b::Int)
    diag[incident(diag, b, :box), :junction]
end
export box_junctions

function neighboring_boxes(diag::UntypedNamedRelationDiagram, b::Int, path::Vector{Int}=Int[])
    junctions_of_box_id = box_junctions(diag, b)
    neighbor = setdiff(diag[vcat(incident(diag, junctions_of_box_id, :junction)...), :box], b)
    setdiff(neighbor, path)
end
export neighboring_boxes

# this should in actuality check neighboring boxes
function valence(diag::UntypedNamedRelationDiagram)
    map(parts(diag, :Box)) do box
        box => (arity=arity(diag, box, :box), neighbors=neighboring_boxes(diag, box))
    end
end
export valence

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
export boxpath

get_box = @relation (variable=variable) begin
    Junction(_id=Junction, variable=variable)
    Port(box=left, junction=Junction)
    Port(box=right, junction=Junction)
end

get_port = @relation (Port=Port, port_name=port_name) begin
    Port(_id=Port, box=box, junction=junction_id, port_name=port_name)
    Junction(_id=junction_id, variable=junction)
end

struct JQParam
    junction::Symbol
    port_name::Symbol
    vals # could be ids
end
export JQParam

function query_boxes(fabric::DataFabric, diagram::UntypedNamedRelationDiagram, left::Int, right::Int; params::Union{JQParam, Vector{JQParam}}=JQParam[])
    # box
    box = diagram[left, :name]
    # params
    js = box_junctions(diagram, left)
    _params = js[arity.(Ref(diagram), js, Ref(:junction)) .== 1]
    param_names = subpart(diagram, _params, :variable)
    newport = setdiff(diagram[incident(diagram, left, :box), :port_name], diagram[incident(diagram, right, :box), :port_name]) 
    #
    _result = Iterators.flatten(incident(fabric, [(jq.vals, jq.port_name) for jq in [JQParam[]; params]])) |> collect
    # _result = incident(fabric, [(params[col], col) for col in param_names])
    # which junction mediates 6, 5
    junct, = js ∩ box_junctions(diagram, right)
    # port_id, = incident(diag, right, :box) ∩ incident(diag, junct, :junction)
    result = query(diagram, get_box, (left=left, right=right))
    junct_name = diagram[junct,:variable]
    port_name = query(diagram, get_port, (box=right,junction=diagram[junct,:variable])).port_name
    JQParam(only(result.variable), only(port_name), _result)
end
export query_boxes

function input_junctions(diagram::UntypedNamedRelationDiagram)
    out = filter(parts(diagram, :Junction)) do j
        j∉(diagram[:outer_junction]) &&
        1==count(==(j), diagram[:junction]) 
    end
    diagram[out, :variable]
end
export input_junctions

function input_junctions(diagram::UntypedNamedRelationDiagram, b::Int)
    junctions = filter(diagram[incident(diagram, b, :box), :junction]) do j
        1==count(==(j), diagram[:junction])
    end
    diagram[junctions, :variable]
end

# query(fabric, diag, (species=:GreenGrape, color=:Green, country=:Italy))
function Catlab.query(fabric::DataFabric, diagram::UntypedNamedRelationDiagram, params=(;); formatter=identity)
    selects, = subpart.(Ref(diagram), incident(diagram, diagram[:outer_junction], :junction), Ref(:port_name))
    outbox, = subpart.(Ref(diagram), incident(diagram, diagram[:outer_junction], :junction), Ref(:box))
    inboxes = first.(filter(((_, v),) -> length(v.neighbors) == 1, valence(diagram)))
    results = map(inboxes) do inbox
        box_params = [JQParam(:_, k, params[k]) for k in input_junctions(diagram, inbox)]
        foldl(boxpath(diagram, inbox, only(outbox)); init=box_params) do param, path
            query_boxes(fabric, diagram, path...; params=param)
        end
    end
    # TODO this implementation prematurely indexes _id from the data frames. 
    # its probably more elegant to have all ACSetInterface
    # functions defined over AbstractDataSource to return a QueryResult object 
    # which lets us implement our own methods for handling
    # cases like this. A simpler solution would be to just return a DataFrame, which has its own methods
    out = incident(fabric, [(jq.vals, jq.port_name) for jq in results]) 
    # out = incident(fabric, intersect(getproperty.(results, :vals)...), :region)
    subpart(fabric, out, selects)
end
