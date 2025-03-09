module Fabric

using ...Schemas

# The DataFabric is an edge-labeled graph of data sources and schema-schema interrelations
# which implements the ACSet interface. It may "virtualize" data by querying it
# into memory.
#
# Stitching: 
# If all the data sources have known database schema, then we can assembly the
# data into a single ACSet schema.
using Catlab
using ACSets

using MLStyle: @match
using Dates
using DataFrames
using DBInterface
using FunSQL
using FunSQL: reflect
import FunSQL: render

# DATA SOURCES
abstract type AbstractDataSource end
export AbstractDataSource

function recatalog! end
export recatalog!

# this is an ACSet
mutable struct InMemory <: AbstractDataSource
    value
    function InMemory(value::AbstractDataSource)
        error("No!")
    end
    function InMemory(value)
        new(value)
    end
end
export InMemory

function recatalog!(x::InMemory) end

# CATALOG

@present SchERD(FreeSchema) begin
    (Name, Conn)::AttrType
    (Source, Table, Column, FK)::Ob

    (to, from)::Hom(FK, Column)
    table::Hom(Column, Table)
    source::Hom(Table, Source)

    tname::Attr(Table, Name)
    (type, cname)::Attr(Column, Name)
    conn::Attr(Source, Conn)
end
@abstract_acset_type AbstractSQLSchema
@acset_type ERD(SchERD) <: AbstractSQLSchema

const Catalog = ERD{Symbol, AbstractDataSource}
export Catalog

function (c::Catalog)(value)
    # TODO pointer to object
    add_part!(c, :Source, conn=Memory(value))
    c
end

function add_to_catalog!(catalog::Catalog, p::Presentation; source=nothing, types::Union{Dict, Nothing}=nothing)
    fields = get_fields(p, types)
    tables = keys(fields)
    id = :SERIAL_PRIMARY_KEY # TODO PostgreSQL
    fk = :INTEGER
    tab2ind = Dict{Symbol, Int64}()
    # load tables into their relations
    source_id = !isnothing(source) ? add_part!(catalog, :Source, conn=source) : 0
    for t in tables
        # TODO upstream
        t_ind = incident(catalog, t, :tname)
        t_ind = isempty(t_ind) ? add_part!(catalog, :Table, tname=t, source=source_id) : only(t_ind)
        c_ind = incident(catalog, t_ind, :table)
        c_ind = isempty(c_ind) ? add_part!(catalog, :Column, table=t_ind, cname=:id, type=id) : only(c_ind)
      tab2ind[t] = t_ind
    end
    #
    for t in tables
      for c in fields[t]
        if c[1] == :Hom
          col = add_part!(catalog, :Column, table = tab2ind[t], cname = c[3], type=fk)
          add_part!(catalog, :FK, from=col, to=tab2ind[c[2]])
        else
          type = type2sql(c[2])
          add_part!(catalog, :Column, table = tab2ind[t], cname = c[3], type=Symbol(type))
        end
      end
    end
    catalog
end
export add_to_catalog!

# END OF CATALOG
# Data Source Graph

# TODO move to Catlab. This is a labeled graph whose edges are also labeled
@present SchEnrichedGraph <: SchLabeledGraph begin
    Value::AttrType
    value::Attr(V, Value)
    EdgeLabel::AttrType
    edgelabel::Attr(E, EdgeLabel)
end
@acset_type EnrichedGraph(SchEnrichedGraph)

const DataSourceGraph = EnrichedGraph{DataType, AbstractDataSource, Pair{Symbol, Symbol}}
export DataSourceGraph

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
    log::Vector{Log} = Log[]
end
export DataFabric

""" accesses the catalog for an abstract data source """
function catalog end
export catalog

catalog(fabric::DataFabric) = fabric.catalog

function recatalog!(fabric::DataFabric)
    foreach(parts(fabric.catalog, :Source)) do i    
        fabric.catalog[i, :conn] = recatalog!(subpart(fabric.catalog, i, :conn))
    end
    fabric
end

# Adding to the Fabric

function add_source!(fabric::DataFabric, source::AbstractDataSource)
    source_id = add_part!(fabric.graph, :V, value=source)
end
export add_source!

function add_table!(fabric::DataFabric, tname::Symbol, source_id::Int=1)
    add_part!(fabric.catalog, :Table, tname=gensym(), source_id=source_id)
end
export add_table!

function add_fk!(fabric::DataFabric, src::Int, tgt::Int, elabel::Pair{Symbol, Symbol})
    add_part!(fabric.graph, :E, src=src, tgt=tgt, edgelabel=elabel)
end
export add_fk!

# Executing commands on data fabric

""" """
function render end
export render

""" """
function execute!(fabric::DataFabric, source_id::Int, stmt)
    execute!(fabric.catalog[source_id, :conn], stmt)
    recatalog!(fabric.catalog[source_id, :conn])
end
export execute!

# ACSet Interface

function decide_source(fabric::DataFabric, attr::Pair{Symbol, Tuple{Symbol, Symbol}})
    id = incident(fabric.catalog, attr.second[1], attr.first)
    source_id = subpart(fabric.catalog, only(id), :source)
    subpart(fabric.catalog, source_id, :conn)
end

function decide_source(fabric::DataFabric, attr::Pair{Symbol, Symbol})
    #
    id = incident(fabric.catalog, attr.second, attr.first)
    if attr.first == :cname
        id = subpart(fabric.catalog, id, :table)
    end
    @assert length(id) == 1
    source_id = subpart(fabric.catalog, id, :source)
    source = subpart(fabric.catalog, source_id, :conn)
    only(source)
end

function ACSetInterface.nparts(fabric::DataFabric, table::Symbol)
    source = decide_source(fabric, :tname => table)
    nparts(source, table) 
end
export nparts

function ACSetInterface.maxpart(fabric::DataFabric, table::Symbol)
    source = decide_source(fabric, :tname => table)
    maxpart(source, table)
end
export maxpart

function ACSetInterface.subpart(fabric::DataFabric, table::Symbol)
    source = decide_source(fabric, :tname => table)
    subpart(source, table)
end
export subpart

function ACSetInterface.subpart(fabric::DataFabric, id, column::Pair{Symbol, Symbol})
    # get columns
    columns = subpart(fabric.catalog, incident(fabric.catalog, column.first, [:table, :tname]), :cname)
    @assert !isempty(columns[columns .== column.second])
    source = subpart(fabric.catalog, 
                     subpart(fabric.catalog, 
                             incident(fabric.catalog, column.first, :tname),
                             :source), 
                     :conn)
    subpart(only(source), id, column.second)
end

function ACSetInterface.incident(fabric::DataFabric, id, column)
    source = decide_source(fabric, :cname => column)
    incident(source, id, column)
end
export incident

function ACSetInterface.incident(fabric::DataFabric, value, tablecol::Tuple{Symbol, Symbol})
    source = decide_source(fabric, :tname => tablecol)
    incident(source, value, tablecol[2])
end

end
