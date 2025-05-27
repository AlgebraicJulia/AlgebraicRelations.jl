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

d = execute!(q)

# TODO have table alias
function render(source::DBSource{SQLite.DB}, wc::WhereCondition)
    "$(wc.lhs[1]).$(wc.lhs[2]) = $(to_sql(source, wc.rhs))"
end

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

# junctions are joins
# outer ports are variables
# ports are columns of a table
# boxes are tables ~ labels

for v in parts(diag, :Junction)
    @info incident(diag, v, :junction)
end


function to_graph(el::Elements)
  F = FinFunctor(Dict(:V => :El, :E => :Arr), Dict(:src => :src, :tgt => :tgt),
                 SchGraph, SchElements)
  ΔF = DataMigrationFunctor(F, Elements{Symbol}, Graph)
  return ΔF(el)
end

"""Enumerate all paths of an acyclic graph, indexed by src+tgt"""
function enumerate_paths(diagram::UntypedNamedRelationDiagram;
                         sorted::Union{AbstractVector{Int},Nothing}=nothing
                        )::ReflexiveEdgePropertyGraph{Vector{Int}}
  
  el = elements(diag)
  G = to_graph(el)

  sorted = topological_sort(G)
  
  _Path = Vector{Int}

  paths = [Set{_Path}() for _ in 1:nv(G)] # paths that start on a particular V
  for v in reverse(sorted)
    push!(paths[v], Int[]) # add length 0 paths
    for e in incident(G, v, :src)
      push!(paths[v], [e]) # add length 1 paths
      for p in paths[G[e, :tgt]] # add length >1 paths
        push!(paths[v], vcat([e], p))
      end
    end
  end

  # Initialize output data structure with empty paths
  res = @acset ReflexiveEdgePropertyGraph{Path} begin
    V=nv(G); E=nv(G); src=1:nv(G); tgt=1:nv(G); refl=1:nv(G)
    eprops=[Int[] for _ in 1:nv(G)]
  end
  for (src, ps) in enumerate(paths)
    for p in filter(x->!isempty(x), ps)
      add_part!(res, :E; src=src, tgt=G[p[end],:tgt], eprops=p)
    end
  end
  return res
end



# TODO add const
mutable struct QueryRopeGraph
    const inputs::Vector{Vector{Int}}
    const paths::Vector{Vector{Int}}
    const arity::OrderedDict{Vector{Int}, Symbol}
    data::Dict{Int,Any}
    function QueryRopeGraph(diagram::UntypedNamedRelationDiagram)
        inputs = []
        paths = []
        arity = OrderedDict(map(reverse(diagram[:variable])) do var
            let boxes = diagram[incident(diagram, var, [:junction, :variable]), :box]
                @info boxes
                @match length(boxes) begin
                    1 => push!(inputs, boxes)
                    2 => push!(paths, boxes)
                    _ => nothing
                end
                boxes => var
            end
        end)
        new(inputs, paths, arity, Dict{Int,Any}())
    end
end


function getpath(diag, port_name::Symbol)

end

i(n,x) = incident(diag,n,x)
s(n,x) = subpart(diag,n,x) |> only

# ## STEP 1
# i1 = i(8, :junction) # [13]
# w1 = s(i1, :box) # 6
# ## STEP 2
# i2 = i(w1, :box) # [12,13]
# w2 = s(i2[i2 .∉ Ref(i1)], :junction) # 7
# i3 = i(w2, :junction) # [11,12]
# w3 = s(i3[i3 .∉ Ref(i2)], :box) # 5
# i4 = i(w3, :box) # [10,11]
# w4 = s(i4[i4 .∉ Ref(i3)], :junction) # 2
# i5 = i(w4, :junction) # [1,10]
# w5 = s(i5[i5 .∉ Ref(i4)], :box) # 1

idx = 2
ports = []
ivars = [i(8, :junction)]
subparts = [s(ivars[1], :box)]
while :winemaker ∉ subpart(diag, ivars[end], :port_name)
    cols = iseven(idx) ? (:box, :junction) : (:junction, :box)
    i_idx = i(subparts[end], cols[1])
    port_idx = i_idx[i_idx .∉ Ref(ivars[end])]
    subpart_idx = s(port_idx, cols[2])
    push!(ivars, i_idx)
    push!(subparts, subpart_idx)
    push!(ports, port_idx)
    idx += 1
end

#     id = i(acc[end], cols[2])
#     i
#     s(i(acc[end], cols[1])[i(acc, cols[1]) .∉ Ref(ids[end])], cols[2])

# all_results[1] is w2, all_results[2] is w3, etc.
w2, w3, w4 = all_results[1:3]
w5 = all_results[end] 

# get input port
input_port = incident(diag, :color, :port_name) |> only # [13]
input_box = diag[input_port, :box] |> only # [6]
# get box
ports_on_box = incident(diag, input_box, :box) # [12,13]
# get junction associated to 12
new_junction = diag[ports_on_box[ports_on_box .!= input_box], :junction] # [7,8] 

foreach(diag[:junction], diag[:box]) do j, b
    @info (j,b)
end

diag = @relation (winemaker=winemaker) begin
    WineWinemaker(wine=wine, winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker)
    CountryClimate(id=region, country=country_id)
    Country(id=country_id, country=name)
    Wine(id=wine, grape=grape)
    Grape(id=grape, color=color)
end

view_graphviz(to_graphviz(diag))

q=QueryRopeGraph(diag)

d=Dict(:country=>:Italy, :color=>:Red)

function Catlab.query(fabric::DataFabric, diagram::UntypedNamedRelatedDiagram, params=(;))
    rope = QueryRopeGraph(diagram)
   
    # TODO
    map([q.paths[1]]) do path
        # first, apply σ or restriction
        rhs=path[2]; lhs=path[1]
        rhs=diagram[rhs,:name] => incident(fabric, d[q.arity[[rhs]]], q.arity[[rhs]])
        lhs=diagram[lhs,:name] => incident(fabric, d[q.arity[[lhs]]], q.arity[[lhs]])
        @info lhs, rhs
    end

end




    # TODO joins
    map([q.paths[1]]) do path
        # first, apply σ or restriction
        rhs=path[2]; lhs=path[1]
        rhs=r[rhs,:name] => incident(fabric, d[q.arity[[rhs]]], q.arity[[rhs]])
        lhs=r[lhs,:name] => incident(fabric, d[q.arity[[lhs]]], q.arity[[lhs]])
        @info lhs, rhs
    end

    function is_leaf(id::Int)
        # get ports incident to this box
        pts=incident(r, id, :box)
        our_junctions=subpart(r, subpart(r, pts, :junction), :variable)
        filter(our_junctions) do junct
            length(arity[junct])!==1
        end
    end

    # filter out ones which correspond to junctions with arity=1
   
    # subpart(r, pts, [:variable, :junction]) TODO does not work


    #

    boxes = Dict([

                  # get the incide
                  box => incident(r, box, :box) ∩ 

    # a box is a "leaf" if only has one inner junction,
    # if it has more than 1 its a branch

    # 1. Grape(species=species)
    # 2. Country(country=Grape#1.country, climate=climate)
    # 3. Winemaker(country_id=Country#2.id) 
