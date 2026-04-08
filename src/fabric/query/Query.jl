struct Encoded
    size::Int
    encoded::Vector{Int}
    unique
end

Base.getindex(encoded::Encoded, idx::Int) = encoded.unique[idx]
Base.getindex(encoded::Encoded, idxs::Vector{Int}) = getindex.(Ref(encoded), idxs)

function encode_attr(acset::ACSet, attr::Symbol)
    vals = acset[attr]
    uniq = unique(vals)
    lookup = Dict(v => i for (i, v) in enumerate(uniq))
    encoded = [lookup[v] for v in vals]
    return Encoded(length(uniq), encoded, uniq)
end

# TODO dispatch on generator
function encode_attr(acset::ACSet)
    s = acset_schema(acset)
    as = attrs(s)
    obs = objects(s)
    ob_attrs = Dict(ob => first.(getindex(as, findall(x->x[2]==ob, as))) for ob in obs)
    Dict(ob => Dict(attr => encode_attr(acset, attr) for attr in ob_attrs[ob]) for ob in obs) 
end
export encode_attr

using ..SQL.InMemoryDS: InMemory 

encode_attr(m::InMemory) = encode_attr(m.value)

using ..SQL.DatabaseDS: DBSource

# TODO encode_attr for db

function encode_attr(acset::DBSource, attr::Symbol)
    vals = subpart(acset,attr) # TODO formatter
    vals = vals[!, attr]
    uniq = unique(vals)
    lookup = Dict(v => i for (i, v) in enumerate(uniq))
    encoded = [lookup[v] for v in vals]
    return Encoded(length(uniq), encoded, uniq)
end

function encode_attr(db::DBSource)
    s = acset_schema(db)
    as = attrs(s)
    obs = objects(s)
    ob_attrs = Dict(ob => first.(getindex(as, findall(x->x[2]==ob, as))) for ob in obs)
    Dict(ob => Dict(attr => encode_attr(db, attr) for attr in ob_attrs[ob]) for ob in obs)
end
#

query_inputs(rel) = [subpart(rel, incident(rel, box, :box), :junction) for box in Catlab.boxes(rel)]

function prepare(rel, data::ACSet, lookup)
    attributes = first.(attrs(acset_schema(data)))
    homs = first.(ACSets.homs(acset_schema(data)))
    outputs = subpart(rel, :outer_junction)
    function g(rel, data, col, box)
        col = subpart(rel, col, :port_name)
        box = subpart(rel, box, :name)
        values = if col ∈ attributes
            lookup[box][col].encoded
        elseif col ∈ homs
            subpart(data, col)
        else
            parts(data, box)
        end
        values'
    end
    inputs = Dict(box => vcat([g(rel, data, col, box) for col in incident(rel, box, :box)]...) for box in Catlab.boxes(rel))
    s = query_inputs(rel)
    dims = Dict(w => 10 for w in 1:maximum(maximum.(s)))
    d = WiringDiagrams.WiringDiagram(s, outputs, dims) 
    a = SpanAlgebra{Matrix{Int}}()
    result = a(d)([inputs[box] for box in Catlab.boxes(rel)]...)
end
export prepare

function prepare(rel, fabric::DataFabric; filters=Dict())
    catalog = fabric.catalog
    tables = subpart(catalog, :tname)
    attributes = (From(:Column=>:cname)|>Where(:type, x->!(x == PK)&&!(x <: FK)))(catalog) |> only |> Base.Fix2(getindex, 2)
    homs = (From(:Column=>:cname)|>Where(:type, x->x<:FK))(catalog) |> only |> Base.Fix2(getindex, 2)
    outputs = subpart(rel, :outer_junction)
    lookup = Dict{Symbol,Dict{Symbol,Encoded}}()
    #
    for box in Catlab.boxes(rel)
        boxname = subpart(rel, box, :name)
        boxname ∈ tables || continue
        data, = subpart(fabric.graph, subpart(catalog, incident(catalog, boxname, :tname), :source), :value)
        encoded = encode_attr(data)
        lookup[boxname] = encoded[boxname]
    end
    #
    function g(rel, fabric, col, box)
        col = subpart(rel, col, :port_name)
        boxname = subpart(rel, box, :name)
        values = @match (box, col) begin
            (_, _) && if boxname ∉ tables end => begin
                col_box, = subpart(catalog, subpart(catalog, incident(catalog, col, :cname), :table), :tname)
                out = [findfirst(lookup[col_box][col].unique .== val) for val in ACSets.Query.iterable(filters[col])]
                out
            end
            (_, _) && if col ∈ attributes end => begin
                if box ∉ keys(lookup)
                    data, = subpart(fabric.graph, subpart(catalog, incident(catalog, boxname, :tname), :source), :value)
                    encoded = encode_attr(data)
                    lookup[boxname] = encoded[boxname]
                end
                lookup[boxname][col].encoded
            end
            (_, _) && if col ∈ homs end => begin
                vals = subpart(fabric, col)
                eltype(vals) <: FK ? getfield.(vals, Ref(:val)) : vals
            end
            _ => begin
                # TODO implement parts for fabric
                data, = subpart(fabric.graph, subpart(catalog, incident(catalog, boxname, :tname), :source), :value)
                if data isa DBSource
                    1:nparts(data,boxname)
                else
                    parts(data, boxname)
                end
            end
        end
        values'
    end
    inputs = Dict(box => vcat([g(rel, fabric, col, box) for col in incident(rel, box, :box)]...) for box in Catlab.boxes(rel))
    s = query_inputs(rel)
    dims = Dict(w => 30 for w in 1:maximum(maximum.(s)))
    d = WD.WiringDiagram(s, outputs, dims) 
    a = WD.SpanAlgebra{Matrix{Int}}()
    result = a(d)([inputs[box] for box in Catlab.boxes(rel)]...)
    (result, lookup)
end

# TODO change Any to AbstractResult

QueryResultDSGraph = DataSourceGraph{Symbol, Union{DataFrame, Nothing}, Symbol}

struct QueryResultWrapper
    qg::QueryResultDSGraph
    # query
end
export QueryResultWrapper

function QueryResultWrapper(g::DataSourceGraph)
    qg = QueryResultDSGraph()
    add_parts!(qg, :V, nparts(g, :V), label=subpart(g, :label))
    edges = parts(g, :E)
    for e in edges
        foot1 = subpart(g, e, :src)
        foot2 = subpart(g, e, :tgt)
        label1 = subpart(g, foot1, :label)
        label2 = subpart(g, foot2, :label)
        apex = add_part!(qg, :V, label=Symbol("$label1⨝$label2"))
        add_parts!(qg, :E, 2, src=[apex, apex], tgt=[foot1, foot2], edgelabel=[label1, label2])
    end
    QueryResultWrapper(qg)
end
export QueryResultWrapper
