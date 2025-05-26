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

diagram = @relation (winemaker=winemaker) begin
    WineWinemaker(wine=wine, winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker)
    CountryClimate(id=region, country=country_id)
    Country(id=country_id, country=name)
    Wine(id=wine, grape=grape)
    Grape(id=grape, color=color)
end

view_graphviz(to_graphviz(diagram))

q=QueryRopeGraph(diagram)

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
