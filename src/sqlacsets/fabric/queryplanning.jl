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

function neighboring_boxes(diag::UntypedNamedRelationDiagram, b::Int64) 
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



