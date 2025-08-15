module DatabaseDS

using ACSets
using ..SQLACSetSyntax
using ..SQLACSetSyntax: AbstractSQLTerm 
using ..Fabric
import ..Fabric: execute!, reconnect!, columntypes

using MLStyle
using DBInterface
using DataFrames

using FunSQL
using FunSQL: SQLTable
using FunSQL: Select, From, Where, Agg, Group, Fun, Get
using FunSQL: FROM, SELECT, WHERE, FUN

@kwdef mutable struct DBSource{Conn} <: AbstractDataSource
    schema::Union{<:BasicSchema, Nothing} = nothing
    conn::FunSQL.SQLConnection{Conn}
    log::Vector{Log} = Log[]
end
export DBSource

function DBSource(conn::Conn, schema=nothing) where Conn
    funconn = FunSQL.DB(conn, catalog=reflect(conn))
    DBSource{Conn}(schema=schema, conn=funconn)
end

Base.nameof(source::DBSource) = nothing

Fabric.catalog(source::DBSource) = source.conn.catalog

# column => type
function Fabric.columntypes(source::DBSource)
    result = get_schema(source)
    Dict([
          Symbol(row.column_name) => row.is_primary_key == 1 ? PK : from_sql(source, row.data_type) for row in eachrow(result)
    ])
end

function Fabric.reconnect!(source::DBSource)
    source.conn = FunSQL.DB(source.conn.raw, catalog=FunSQL.reflect(source.conn.raw))
    source
end
export reconnect!

function Fabric.execute!(db::DBSource, stmt::AbstractString, formatter=DataFrame)
    result = DBInterface.execute(db.conn.raw, stmt)
    reconnect!(db)
    isnothing(formatter) && return result
    formatter(result)
end

# TODO could probably implement `isDML(::AbstractSQLTerm) = true` for types that are
function Fabric.execute!(db::DBSource, stmt::AbstractSQLTerm, formatter=DataFrame)
    # @match statement because of DBInterface.execute
    result = @match stmt begin
        ::ACSetInsert || ::ACSetUpdate => DBInterface.execute(db.conn.raw, render(db, stmt))
        _ => DBInterface.execute(db.conn, render(db, stmt))
    end
    reconnect!(db)
    isnothing(formatter) && return result
    formatter(result)
end
export execute!

DenseACSets.acset_schema(db::DBSource) = db.schema


include("acsets_interface.jl")

end
