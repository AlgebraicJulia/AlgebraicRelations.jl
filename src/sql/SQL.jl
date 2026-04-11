module SQL

using Reexport

using ACSets

# Every database has
# - init : Connection -> Schema -> ()
# - prepare: Connection -> Query -> DF
# - execute: Connection -> Query -> DF
# - execute: Statement -> AbstractArray -> DF
# ...uploading a csv means inserting...
using TraitInterfaces

@interface ThDatabase begin
    Data::TYPE
    Connection::TYPE
    Query::TYPE
    prepare(conn::Connection, q::Query)::Data
    execute(conn::Connection, q::Query)::Data
end

function sql end
export sql

# TODO delete
TypeToSQL = Dict("String" => "TEXT",
               "Int" => "INTEGER",
               "Int64" => "INTEGER",
               "IntArray" => "INTEGER[]",
               "FloatMatrix" => "INTEGER[][]",
               "Float64" => "REAL",
               "FloatArray" => "REAL[]",
               "FloatMatrix" => "REAL[][]",
               "Bool" => "BOOLEAN",
               "Date" => "DATE")

sql(::Type{<:Number}) = "REAL"
sql(::Type{<:Vector{<:Number}}) = "REAL[]"
sql(::Type{<:Matrix{<:Number}}) = "REAL[][]"
sql(::Type{<:Int}) = "INTEGER"
sql(::Type{<:Vector{<:Int}}) = "INTEGER[]"
sql(::Type{<:Matrix{<:Int}}) = "INTEGER[][]"
sql(::Type{<:String}) = "TEXT"
sql(s::Symbol) = sql("$s")
sql(s::String) = s ∈ keys(TypeToSQL) ? TypeToSQL[s] : "TEXT"

include("Syntax.jl")
include("Schemas.jl")

@reexport using .Syntax
@reexport using .Schemas

function columntypes end
export columntypes

function encode_attr end
export encode_attr

# DATA SOURCES

# TODO derive as a trait
@interface ThDataSource begin
    @import ACSet::TYPE
    @import Vector::TYPE
    Statement::TYPE
    Source::TYPE # Type of data 
    reconnect!(s::Source)::Source
    # incident(s::Source, r::Row, c::Column)::Vector{Row}
    execute!(d::Source, stmt::Statement)::ACSet # TODO stmt, formatter
    # execute!(d::Source, stmt::AbstractSQLTerm)::ACSet
    schema(d::Source)::ACSet
end
export ThDataSource, reconnect!, execute!

abstract type AbstractDataSource end
export AbstractDataSource

get_schema(::AbstractDataSource) = []
export get_schema

struct Encoded
    size::Int
    encoded::Vector{Int}
    unique
end

Base.getindex(encoded::Encoded, idx::Int) = encoded.unique[idx]
Base.getindex(encoded::Encoded, idxs::Vector{Int}) = getindex.(Ref(encoded), idxs)

include("database/DatabaseDS.jl")
include("inmemory/InMemoryDS.jl")

# include("WebApiDS.jl")

@reexport using .DatabaseDS
@reexport using .InMemoryDS

end
