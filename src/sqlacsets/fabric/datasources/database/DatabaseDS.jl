module DatabaseDS

using ACSets
using ..SQLACSetSyntax
using ..SQLACSetSyntax: AbstractSQLTerm 
using ..Fabric
import ..Fabric: execute!

using MLStyle
using DBInterface
using DataFrames

using FunSQL
using FunSQL: SQLTable, reflect
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

Fabric.catalog(db::DBSource) = db.conn.catalog

function Fabric.recatalog!(db::DBSource)
    db.conn = FunSQL.DB(db.conn.raw, catalog=reflect(db.conn.raw))
    db
end
export recatalog!

function Fabric.execute!(db::DBSource, stmt::AbstractString; formatter=DataFrame)
    result = DBInterface.execute(db.conn.raw, stmt)
    isnothing(formatter) && return result
    formatter(result)
end

function Fabric.execute!(db::DBSource, stmt::AbstractSQLTerm; formatter=DataFrame)
    # @match statement because of DBInterface.execute
    result = @match stmt begin
        ::ACSetInsert => DBInterface.execute(db.conn.raw, render(db, stmt))
        _ => DBInterface.execute(db.conn, render(db, stmt))
    end
    isnothing(formatter) && return result
    formatter(result)
end
export execute!

DenseACSets.acset_schema(db::DBSource) = db.schema

include("acsets_interface.jl")

end
