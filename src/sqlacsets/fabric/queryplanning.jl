using ACSets

using OrderedCollections: OrderedDict
using MLStyle: @match, @λ
using ACSets.Query
import ACSets.Query: WhereCondition, AndWhere, OrWhere
using Catlab
using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

struct TableConds
    conds::Dict{Vector{Symbol}, Vector{WhereCondition}}
end

function TableConds(q::ACSets.Query.ACSetSQLNode)
    result = Dict{Vector{Symbol}, Vector{WhereCondition}}()
    walk = @λ begin
        wheres::Vector{ACSets.Query.AbstractCondition} -> walk.(wheres)
        boolean::Union{AndWhere, OrWhere} -> walk.(boolean.conds)
        wc::WhereCondition -> begin
            out = Symbol[]
            lhs = (length(wc.lhs) > 1 && !(wc.lhs isa String)) ? push!(out, wc.lhs[1]) : nothing
            rhs = (length(wc.rhs) > 1 && !(wc.rhs isa String)) ? push!(out, wc.rhs[1]) : nothing
            haskey(result, out) ? setindex!(result, [result[out]; [wc]], out) : push!(result, out => [wc])
        end
        _ => nothing
    end
    walk(q.cond)
    TableConds(result)
end
## render to 


function ACSetInterface.incident(fabric::DataFabric, wc::WhereCondition)
    incident(fabric, Symbol(wc.rhs), wc.lhs[2])
end

@present SchQueryRope <: SchLabeledGraph begin
    Data::AttrType
    data::Attr(V, Data)
end
@acset_type QueryRope(SchQueryRope)

using StructEquality

@struct_hash_equal struct Field
    x
end

function Fabric.execute!(fabric::DataFabric, q::ACSets.Query.ACSetSQLNode)
    d = TableConds(q)
    # query independent tables
    tables = filter(x -> length(x) == 1, keys(d.conds))
    # get their results. we assume "OR" right now
    itr = Dict(Iterators.map(tables) do table
        [only(table), :id] => collect(Iterators.flatten(incident.(Ref(fabric), d.conds[table])))
    end)
    # if a key is an adhesion, then query the easiest one, 
    # and pass the ids in as a where statement
    adh_tables = filter(x -> length(x) == 2, keys(d.conds))
    #
    tablefields = Iterators.map(adh_tables) do table
        rhs = getfield.(d.conds[table], :rhs)
        lhs = getfield.(d.conds[table], :lhs)
        # itr[first(rhs)] gets the ids from the `itr` variable
        # this returns the _ids of the matchs
        df = incident(fabric, itr[first(rhs)], first(lhs)[2])
        # @info df, first(lhs)[2]
        table => df
        # table => subpart(fabric, df._id, :country)
    end |> collect
    # get the table associated to the righthand WhereCondition
    # select the RHS.col where the ids agree
end

# function getpath(diag, box::Int)
#     i(n, x) = incident(diag, n, x)
#     i(
# end

function getpath(diag, junction_name::Symbol, input::Int)
    i(n,x) = incident(diag,n,x)
    s(n,x) = begin
        out = subpart(diag,n,x)
        out |> only
    end
    idx = 2
    ports = Int[]
    ivars = [i(input, :junction)]
    subparts = [input, s(ivars[1], :box)]
    boxes = [subparts[end]]
    junction_spans = []
    while junction_name ∉ subpart(diag, subpart(diag, ivars[end], :junction), :variable)
        cols = iseven(idx) ? (:box, :junction) : (:junction, :box)
        i_idx = i(subparts[end], cols[1])
        if cols[1] == :junction
            push!(junction_spans, subparts[end] => i_idx)
        end
        port_idx = i_idx[i_idx .∉ Ref(ivars[end])]
        junction_name ∈ subpart(diag, subpart(diag, i_idx, :junction), :variable) && break
        subpart_idx = s(port_idx, cols[2])
        if cols[2] == :box
            push!(boxes, subpart_idx)
        end
        push!(ivars, i_idx)
        push!(subparts, subpart_idx)
        push!(ports, only(port_idx))
        idx += 1
    end
    (ports=ports, ivars=ivars, subparts=subparts, boxes=boxes, junction_spans=junction_spans)
end

function getpath(diag, junction_name::Symbol, input::Symbol)
    junct_id = incident(diag, input, :variable)
    getpath(diag, junction_name, first(junct_id))
end

mutable struct QueryRopeGraph
    const inputs::Vector{Int}
    const paths::Dict{Symbol, Any}
    const arity::OrderedDict{Vector{Int}, Symbol}
    data::Dict{Int,Any}
    function QueryRopeGraph(diagram::UntypedNamedRelationDiagram)
        inputs = Int[]
        arity = OrderedDict(map(reverse(diagram[:variable])) do var
            let boxes = diagram[incident(diagram, var, [:junction, :variable]), :box]
                if length(boxes) == 1
                    let junct_id = first(incident(diagram, var, :variable))
                        if isempty(incident(diagram, junct_id, :outer_junction))
                            push!(inputs, first(incident(diagram, var, :variable)))
                        end
                    end
                end
                boxes => var
            end
        end)
        paths = Dict([diagram[input, :variable] => getpath(diagram, :winemaker_name, input) for input in inputs])
        new(inputs, paths, arity, Dict{Int,Any}())
    end
end
export QueryRopeGraph

d=Dict(:country=>:Italy, :color=>:Red)

# function Catlab.query(fabric::DataFabric, diagram::UntypedNamedRelatedDiagram, params=(;))
#     rope = QueryRopeGraph(diagram)
   
#     # TODO
#     map([q.paths[1]]) do path
#         # first, apply σ or restriction
#         rhs=path[2]; lhs=path[1]
#         rhs=diagram[rhs,:name] => incident(fabric, d[q.arity[[rhs]]], q.arity[[rhs]])
#         lhs=diagram[lhs,:name] => incident(fabric, d[q.arity[[lhs]]], q.arity[[lhs]])
#         @info lhs, rhs
#     end

# end


function arity(diag::UntypedNamedRelationDiagram, i::Int, label::Symbol=:junction)
    length(incident(diag, i, label))
end
export arity

function box_junctions(diag::UntypedNamedRelationDiagram, b::Int)
    diag[incident(diag, b, :box), :junction]
end
export box_junctions

function neighboring_boxes(diag::UntypedNamedRelationDiagram, b::Int, path::Vector{Int}=Int[])
    junctions_of_box_id = box_junctions(diag, b) # 7, 8, 9
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
export boxpath

get_box=@relation (variable=variable) begin
    Junction(_id=Junction, variable=variable)
    Port(box=left, junction=Junction)
    Port(box=right, junction=Junction)
end

get_port=@relation (Port=Port, port_name=port_name) begin
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
export query_boxes

