"""
The DataFabric is an edge-labeled graph of data sources and schema-schema interrelations
which implements the ACSet interface. It may "virtualize" data by querying it
into memory.
"""
module Fabric

using ..SQL: SQLSchema, ThDataSource, AbstractDataSource, columntypes, execute!
using ..SQL.Syntax
using ...AlgebraicRelations: Log, Maybe
import ...AlgebraicRelations: trait

# ## Colimiting: 
# If all the data sources have known database schema, then we can assembly the
# data into a single ACSet schema.
using Catlab
using Catlab.Graphics.Graphviz
using ACSets

using TraitInterfaces: @instance

using MLStyle: @match, @as_record
using Dates
using DataFrames
using DBInterface
using FunSQL
using FunSQL: reflect
import FunSQL: render

using PrettyTables
using StructEquality
using Reexport

struct PK end
export PK

get_sqlite_schema(::Any) = []
export get_sqlite_schema

# foreign key wrapper
# TODO as_record
@struct_hash_equal struct FK{T<:ACSet} 
    val::Int
end
export FK

function from_sql end
export from_sql

include("catalog.jl")
# Data Source Graph

# TODO move to Catlab. This is a labeled graph whose edges are also labeled
@present SchEnrichedGraph <: SchLabeledGraph begin
    Value::AttrType
    value::Attr(V, Value)
    EdgeLabel::AttrType
    edgelabel::Attr(E, EdgeLabel)
end
@acset_type DataSourceGraph(SchEnrichedGraph)

DataSourceGraph() = DataSourceGraph{Symbol, AbstractDataSource, Pair{Symbol, Symbol}}()
export DataSourceGraph

Catlab.src(g::DataSourceGraph, e::Int) = subpart(g, e, :src)
Catlab.tgt(g::DataSourceGraph, e::Int) = subpart(g, e, :tgt)

function Catlab.to_graphviz(g::DataSourceGraph)::Graphviz.Graph
    gv_name(v::Int) = "$(something(subpart(g, v, :label), :a))"
    gv_path(e::Int) = [gv_name(src(g,e)), gv_name(tgt(g,e))]
    stmts = Graphviz.Statement[]
    for v in parts(g, :V)
        push!(stmts, Graphviz.Node(gv_name(v), Dict()))
    end
    for e in parts(g, :E)
        push!(stmts, Graphviz.Edge(gv_path(e), Dict()))
    end
    # attrs = gprops(g)
    Graphviz.Graph(
      name = "G",
      directed = true,
      # prog = get(attrs, :prog, is_directed ? "dot" : "neato"),
      stmts = stmts,
      # graph_attrs = get(attrs, :graph, Dict()),
      # node_attrs = get(attrs, :node, Dict()),
      # edge_attrs = get(attrs, :edge, Dict()),
    )
end

function Catlab.Graphics.Graphviz.view_graphviz(g::DataSourceGraph)
    view_graphviz(to_graphviz(g))
end
export view_graphviz

using TraitInterfaces
import Catlab: ACSet

include("query/Query.jl")

@kwdef mutable struct DataFabric
    # this will store the connections, their schema, and values
    graph::DataSourceGraph = DataSourceGraph()
    catalog::Catalog = Catalog()
    queries::Vector{QueryResultWrapper} = QueryResultWrapper[]
    log::Vector{Log} = Log[]
end
export DataFabric

""" accesses the catalog for an abstract data source """
catalog(fabric::DataFabric) = fabric.catalog
export catalog

""" accesses the queries for a given data fabric """
queries(fabric::DataFabric) = fabric.queries
export queries

struct FabricTrait end 
trait(::DataFabric) = FabricTrait()

struct Statement
    src::Int
    stmt::String
end
export Statement

TraitInterfaces.@instance ThDataSource{Source=DataFabric,Statement=Statement} [model::FabricTrait] begin
    """ Reconnect to all data sources on nodes """
    function reconnect!(fabric::DataFabric)
        foreach(parts(fabric.graph, :V)) do i
            value = subpart(fabric.graph, i, :value)
            τ = trait(value) 
            fabric.graph[i, :value] = reconnect![τ](value)
        end
        fabric
    end
    """ TODO """
    function execute!(fabric::DataFabric, stmt::Statement)
        node = fabric.graph[stmt.src, :value]
        execute![trait(node)](node, stmt.stmt)
    end
    """ TODO """
    function schema(fabric::DataFabric)
        nothing
    end
end
export reconnect!, execute!, schema

function reflect_source!(fabric::DataFabric, vs::Vector{Int})
    foreach(vs) do source_id
        source = subpart(fabric.graph, source_id, :value)
        # TODO XXX InMemory and DBSource will have different acset_schemas
        schema = acset_schema(source)
        schema = schema isa SQLSchema ? schema : SQLSchema(Presentation(schema))
        # schema = SQLSchema(Presentation(acset_schema(source)))
        types = columntypes(source)
        add_to_catalog!(fabric.catalog, schema; source=source_id, conn=typeof(source), types=types)
    end
end

reflect_source!(fabric, vs::UnitRange{Int64}) = reflect_source!(fabric, collect(vs))

function reflect_edges!(fabric::DataFabric, es::Vector{Int})
    # TODO improve this
    foreach(es) do edge_id
        src, tgt, edgelabel = subpart.(Ref(fabric.graph), edge_id, [:src, :tgt, :edgelabel])
        # gets table associated to source
        fromtable, fromcol = split("$(edgelabel.first)", "!") # TODO i dislike this (de-)munging
        totable, tocol = split("$(edgelabel.second)", "!")
        from = only(incident(fabric.catalog, Symbol(fromcol), :cname))
        to = only(incident(fabric.catalog, Symbol(tocol), :cname))
        # check if it should be added
        check1 = incident(fabric.catalog, to, :to)
        check2 = incident(fabric.catalog, from, :from)
        if check1 == [] && check2 == []
            add_part!(fabric.catalog, :FK, to=to, from=from)
        end
    end
end

reflect_edges!(fabric, vs::UnitRange{Int64}) = reflect_edges!(fabric, collect(vs))

# TODO don't want copy sources , TODO need idempotence
""" """
function reflect!(fabric::DataFabric; source_id::Maybe{Int}=nothing, edge_id::Maybe{Int}=nothing)
    vs = isnothing(source_id) ? isnothing(edge_id) ? parts(fabric.graph, :V) : Int[] : [source_id]
    reflect_source!(fabric, vs)
    es = isnothing(edge_id) ? isnothing(source_id) ? parts(fabric.graph, :E) : Int[] : [edge_id]
    reflect_edges!(fabric, es)
    catalog(fabric)
end
export reflect!

# mutators 
function add_source!(fabric::DataFabric, source::AbstractDataSource, label=nameof(source))
    source_id = add_part!(fabric.graph, :V, label=label, value=source)
    reflect!(fabric; source_id=source_id)
    source_id
end
export add_source!

function add_table!(fabric::DataFabric, tname::Symbol, source_id::Int=1)
    add_part!(fabric.catalog, :Table, tname=gensym(), source_id=source_id)
end
export add_table!

function add_fk!(fabric::DataFabric, src::Int, tgt::Int, elabel::Pair{Symbol, Symbol})
    edge_id = add_part!(fabric.graph, :E, src=src, tgt=tgt, edgelabel=elabel)
    reflect!(fabric; edge_id=edge_id)
    edge_id
end
export add_fk!


# Executing commands on data fabric

""" """
function render end
export render

# ACSet Interface for the Fabric. It determines which data source to dispatch the ACSet function on
include("acset_interface.jl")
include("queryplanning.jl")

# Custom show methods for Fabric objects
include("show.jl")


end
