using MLStyle.Modules.Cond

using DataFrames
using FunSQL: reflect

function tostring end
export tostring

abstract type AbstractVirtualACSet end

#### Reading large data materialized elsewhere
@kwdef mutable struct VirtualACSet{Conn}
    conn::FunSQL.SQLConnection{Conn}
    acsettype::Union{Type{<:ACSet}, Nothing} = nothing
    view::Union{DataFrames.DataFrame, Nothing} = nothing
end
export VirtualACSet
# TODO we need to convert the `view` into an ACSet

function VirtualACSet(conn::FunSQL.SQLConnection{Conn}, acs::A) where {Conn, A<:ACSet}
    VirtualACSet{Conn}(conn=conn, acsettype=typeof(acs))
end

function VirtualACSet(conn::Conn, acs::SimpleACSet) where {Conn<:DBInterface.Connection}
    c = FunSQL.DB(conn, catalog=reflect(conn))
    VirtualACSet{Conn}(conn=c, acsettype=typeof(acs))
end

Base.show(io::IOBuffer, v::VirtualACSet) = println(io, "$(v.conn)\n$(v.view)")

# how do we know which view we are looking at?

# blends homs and attrs together. not idea
function namesrctgt(schema::BasicSchema)
    Dict([name => src => tgt for (name, src, tgt) in schema.homs ∪ schema.attrs])
end

function toacset(vas::VirtualACSet{Conn}) where Conn
    acset = vas.acsettype()
    isnothing(vas.view) && return acset
    schema = acset_schema(acset)
    # get columns in schema
    kvs = namesrctgt(schema)
    # exclude primary key column
    subview = vas.view[!, Not(:_id)]
    cols = propertynames(subview)
    ob = first.(getindex.(Ref(kvs), cols)) |> unique |> only
    # instantiate 
    add_parts!(acset, ob, nrow(vas.view))
    for (name, column) in pairs(eachcol(subview))
        fill!(acset, kvs[name].second, Int64(maximum(column))) # whattabout symbols?
        set_subpart!(acset, name, Int64.(vas.view[!, name]))
    end
    acset
end
export toacset

function reload! end
export reload!

# TODO generate multiple statements, then decide to execute single or multiple
function execute!(vas::VirtualACSet{Conn}, stmt::String) where Conn
    result = DBInterface.execute(vas.conn, stmt)
    reload!(vas)
    DataFrames.DataFrame(result)
end
export execute!

function execute!(vas::VirtualACSet{Conn}, query::SQLTerms) where Conn
    result = @match query begin
        ::ACSetInsert => DBInterface.execute(vas.conn.raw, tostring(vas, query))
        _ => DBInterface.execute(vas.conn, tostring(vas, query))
    end
    DataFrames.DataFrame(result)
end

function ACSet!(vas::VirtualACSet{Conn}, query::SQLTerms) where Conn
    vas.view = execute!(vas, query)
    ACSet(vas)
end

function create!(conn::DBInterface.Connection, x::SimpleACSet)
    stmt = tostring(conn, Create(x))
    DBInterface.executemultiple(conn, stmt)
end
export create!

function create!(v::VirtualACSet{Conn}) where Conn
    query = tostring(v.conn, v.acsettype)
    DBInterface.execute(v.conn, query)
end

function insert!(v::VirtualACSet{Conn}, acset::SimpleACSet) where Conn
    insert_stmts = tostring.(Ref(v.conn), Insert(v.conn, acset))
    query = DBInterface.executemultiple(conn, insert_stmts)
    DataFrames.DataFrame(query)
end

function update!(v::VirtualACSet, acset::SimpleACSet)
    update_stmts = to_string(v.conn, Update(v.conn, acset))
    query = DBInterface.executemultiple(conn, update_stmts)
    DataFrames.DataFrame(query)
end

function tosql end
export tosql

""" """
function entuple(v::Values; f::Function=identity, values::Bool=true)
    ["($(join(f.(vals), ",")))" for vals in values.(v.vals)]
end
export entuple

# get attrs
function getattrs(g::SimpleACSet, table::Symbol)
    first.(filter(attrs(acset_schema(g))) do (attr, tbl, _)
        table == tbl
    end)
end
export getattrs

gethoms(x::SimpleACSet, table::Symbol) = first.(homs(acset_schema(x); from=table))
export gethoms

# Values should have a method which turns single values into "(1)"
function getrows(conn::Conn, x::SimpleACSet, table::Symbol) where Conn <: DBInterface.Connection
    cols = gethoms(x, table) ∪ getattrs(x, table)
    x = map(parts(x, table)) do id
        (;zip([:_id, cols...], [id, tosql.(Ref(conn), subpart.(Ref(x), Ref(id), cols))...])...)
    end
    Values(table, x)
end
export getrows

# FIXME Set
function colnames(x::SimpleACSet, table::Symbol)
    homnames = first.(homs(acset_schema(x); from=table))
    gattrs = getattrs(x, table)
    # I don't like this as it assumes the order of the columns would agree
    cols = [:_id, (homnames ∪ gattrs)...]
    """($(join(cols, ", ")))"""
end
export colnames

function wrap(stmt::String, left::String, right::String)
    join([left, stmt, right], " ")
end
export wrap
