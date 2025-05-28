module Fabric

using ...Schemas
using ..SQLACSetSyntax

# The DataFabric is an edge-labeled graph of data sources and schema-schema interrelations
# which implements the ACSet interface. It may "virtualize" data by querying it
# into memory.
#
# ## Colimiting: 
# If all the data sources have known database schema, then we can assembly the
# data into a single ACSet schema.
using Catlab
using Catlab.Graphics.Graphviz
using ACSets

using MLStyle: @match, @as_record
using Dates
using DataFrames
using DBInterface
using FunSQL
using FunSQL: reflect
import FunSQL: render

using StructEquality
using Reexport

function columntypes end
export columntypes

struct PK end
export PK

# foreign key wrapper
# TODO as_record
@struct_hash_equal struct FK{T<:ACSet} 
    val::Int
end
export FK

# DATA SOURCES
abstract type AbstractDataSource end
export AbstractDataSource

get_schema(::AbstractDataSource) = []
export get_schema

function to_sql end
export to_sql

function from_sql end
export from_sql

function recatalog! end
export recatalog!

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

# DataFabric
struct Log
    time::DateTime
    event
    Log(event::DataType) = new(Dates.now(), event)
end
export Log

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

""" pointwise recataloging of nodes """
function recatalog!(fabric::DataFabric)
    foreach(parts(fabric.graph, :V)) do i    
        fabric.graph[i, :value] = recatalog!(subpart(fabric.graph, i, :value))
    end
    fabric
end

function reflect_source!(fabric::DataFabric, vs::Vector{Int})
    foreach(vs) do source_id
        source = subpart(fabric.graph, source_id, :value)
        schema = SQLSchema(Presentation(acset_schema(source)))
        types = columntypes(source)
        add_to_catalog!(fabric.catalog, schema; source=source_id, conn=typeof(source), types=types)
    end
end

function reflect_edges!(fabric::DataFabric, es::Vector{Int})
    # TODO improve this
    foreach(es) do edge_id
        src, tgt, edgelabel = subpart.(Ref(fabric.graph), edge_id, [:src, :tgt, :edgelabel])
        # gets table associated to source
        fromtable, fromcol = split("$(edgelabel.first)", "!")
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


# TODO don't want copy sources , TODO need idempotence
function reflect!(fabric::DataFabric; source_id::Union{Int, Nothing}=nothing, edge_id::Union{Int, Nothing}=nothing)
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

""" """
function execute!(fabric::DataFabric, source_id::Int, stmt)
    execute!(fabric.graph[source_id, :value], stmt)
    # recatalog!(fabric.catalog[source_id, :conn])
end
export execute!

# ACSet Interface for the Fabric. It determines which data source to dispatch the ACSet function on
include("acset_interface.jl")
include("queryplanning.jl")

include("datasources/database/DatabaseDS.jl")
include("datasources/inmemory/InMemoryDS.jl")

@reexport using .DatabaseDS
@reexport using .InMemoryDS


end
