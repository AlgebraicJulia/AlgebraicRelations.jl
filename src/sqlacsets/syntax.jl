using DataFrames
using DBInterface
using MySQL

function tostring end
export tostring

tostring(conn, nothing) = ""

# this typing ensures that named tuples have the same keys
struct Values{T}
    table::Union{Symbol, Nothing}
    vals::Vector{<:NamedTuple{T}}
end

Base.length(v::Values{T}) where T = length(v.vals)
Base.iterate(v::Values{T}, args...) where T = iterate(v.vals, args...)
Base.broadcast(f, v::Values{T}) where T = Values{T}(v.table, broadcast(f, v.vals))

columns(v::Values{T}) where T = T
export columns

@as_record struct WhereClause
    operator::Symbol
    clauses::Union{Pair{Symbol, <:Any}, Vector{<:WhereClause}}
end
export WhereClause

@kwdef struct SQLEquation
    lhs::Pair{Symbol, Symbol}
    rhs::Pair{Symbol, Symbol}
    op::Symbol = :(==)
end
export SQLEquation

SQLEquation(lhs, rhs) = SQLEquation(lhs=lhs, rhs=rhs)

# select expr from dual;
# SQLite supports SELECT * FROM A, B, C ON (A.x = B.y AND B.y = C.z)
# select * from t1 inner join

@data SQLSelectQuantity begin
    SelectAll() # default
    SelectDistinct()
    SelectDistinctRow()
    # Pair{Symbol, Symbol} is table.column relation
    SelectColumns(::Vector{Union{Symbol, Pair{Symbol, Symbol}}})
end
export SQLSelectQuantity, SelectAll, SelectDistinct, SelectDistinctRow, SelectColumns

SelectColumns(t::Union{Symbol, Pair{Symbol, Symbol}}) = SelectColumns([t])
SelectColumns(varargs...) = SelectColumns([varargs...])

function SelectColumns(t::Vector{Pair{Symbol, Any}})
    xs = Vector{Pair{Symbol, Symbol}}()
    foreach(t) do (k, v)
        v isa Symbol ? push!(xs, k => v) : push!.(Ref(xs), (=>).(Ref(k), v))
    end
    SelectColumns(xs)
end

struct ACSetJoin
    type::Symbol
    table::Symbol
    on::Union{Vector{SQLEquation}, Nothing}
    function ACSetJoin(type::Symbol, table::Symbol, on::SQLEquation)
        new(type, table, [on])
    end
end
export ACSetJoin

@data SQLTerms begin
    ACSetInsert(table::Symbol, values::Values, wheres::Union{WhereClause, Nothing})
    ACSetUpdate(table::Symbol, values::Values, wheres::Union{WhereClause, Nothing})
    ACSetSelect(qty::SQLSelectQuantity, 
        from::Union{Symbol, Vector{Symbol}}, # TODO could be subquery 
        join::Union{ACSetJoin, Nothing},
        wheres::Union{WhereClause, Nothing})
    ACSetAlter(table::Symbol, refdom::Symbol, refcodom::Symbol)
    ACSetCreate(schema::BasicSchema{Symbol})
    ACSetDelete(table::Symbol, ids::Vector{Int})
end
export SQLTerms, Values, ACSetInsert, ACSetUpdate, ACSetSelect, ACSetAlter, ACSetCreate, ACSetDelete

## Constructors

function ACSetSelect(from::Union{Symbol, Vector{Symbol}}; 
        what::SQLSelectQuantity=SelectAll(),
        on::Union{Vector{SQLEquation}, Nothing}=nothing, 
        wheres::Union{WhereClause, Nothing}=nothing)
    ACSetSelect(what, from, on, wheres)
end

function ACSetAlter(table::Symbol, arrow::Pair{Symbol, Symbol})
    ACSetAlter(table, arrow.first, arrow.second)
end

function ACSetCreate(acset::SimpleACSet)
    ACSetCreate(acset_schema(acset))
end

function ACSetInsert(table::Symbol, vs::Vector{<:NamedTuple{T}}, wheres::Union{WhereClause, Nothing}=nothing) where T
    ACSetInsert(table, Values(table, vs), wheres)
end

function ACSetUpdate(table::Symbol, vs::Vector{<:NamedTuple{T}}, wheres::Union{WhereClause, Nothing}=nothing) where T
    ACSetUpdate(table, Values(table, vs), wheres)
end

abstract type DatabaseEnvironmentConfig end

struct ForeignKeyChecks <: DatabaseEnvironmentConfig 
    bool::Bool
end
export ForeignKeyChecks
