module DatabaseDS

using ..SQL: AbstractDataSource, ThDatabase, ThDataSource, Syntax, Schemas
import ..SQL: columntypes
using ...AlgebraicRelations
using ...AlgebraicRelations: Log

using ACSets

using MLStyle
using DBInterface
using DataFrames
using TraitInterfaces

using FunSQL
using FunSQL: SQLTable
using FunSQL: Select, From, Where, Agg, Group, Fun, Get
using FunSQL: FROM, SELECT, WHERE, FUN

@kwdef mutable struct DBSource{Conn} <: AbstractDataSource
    # TODO needs to be consistent
    schema::Union{<:BasicSchema, <:Schemas.SQLSchema, Nothing} = nothing
    conn::FunSQL.SQLConnection{Conn}
    types::Dict{Symbol,DataType} = Dict{Symbol,DataType}() 
    log::Vector{Log} = Log[]
end
export DBSource

function DBSource(conn::Conn, data::ACSet) where Conn
    funconn = FunSQL.DB(conn, catalog=FunSQL.reflect(conn))
    schema = acset_schema(data)
    types = Dict(col => type for (col, type) in zip(attrtypes(schema), [typeof(data).parameters...]))
    DBSource{Conn}(schema=schema, conn=funconn, types=types)
end

function DBSource(conn::Conn, schema=nothing) where Conn
    funconn = FunSQL.DB(conn, catalog=FunSQL.reflect(conn))
    DBSource{Conn}(schema=schema, conn=funconn)
end

struct DBSourceTrait end
AlgebraicRelations.trait(::DBSource) = DBSourceTrait() 

TraitInterfaces.@instance ThDataSource{Source=DBSource, Statement=AbstractString} [model::DBSourceTrait] begin
    function reconnect!(source::DBSource)
        source.conn = FunSQL.DB(source.conn.raw, catalog=FunSQL.reflect(source.conn.raw))
        source
    end
    function execute!(source::DBSource, stmt::AbstractString)
        result = DBInterface.execute(source.conn.raw, stmt)
        reconnect![model](source)
        DataFrame(result)
    end
    function schema(source::DBSource)
        source.schema
    end
end

Base.nameof(source::DBSource) = nothing

# TODO
# Fabric.catalog(source::DBSource) = source.conn.catalog

# column => type
function columntypes(source::DBSource)
    result = get_schema(source)
    s = acset_schema(source)
    ats = attrs(s)
    Dict(Symbol(row.column_name) => @match row begin
        row && if row.is_primary_key == 1 end => PK
        _ && if Symbol(row.column_name) ∈ getindex.(ats, Ref(1)) end => begin
            attr, = ats[getindex.(ats, Ref(1)) .== Symbol(row.column_name)]
            T = source.types[attr[3]]
            T <: FK ? T : from_sql(source, row.data_type)
        end
        _ => from_sql(source, row.data_type)
     end 
     for row in eachrow(result))
end

# TODO could probably implement `isDML(::AbstractSQLTerm) = true` for types that are
# function Fabric.execute!(db::DBSource, stmt::AbstractSQLTerm, formatter=DataFrame)
#     # @match statement because of DBInterface.execute
#     result = @match stmt begin
#         # ::ACSetInsert || ::ACSetUpdate => DBInterface.execute(db.conn.raw, render(db, stmt))
#         _ => DBInterface.execute(db.conn, render(db, stmt))
#     end
#     reconnect!(db)
#     isnothing(formatter) && return result
#     formatter(result)
# end
# export execute!

DenseACSets.acset_schema(db::DBSource) = db.schema

include("acsets_interface.jl")

end
