#####################
### ACSets Interface
######################
using ACSets

using DBInterface
using FunSQL
using FunSQL: SQLTable
using FunSQL: Select, From, Where, Agg, Group, Fun, Get
using FunSQL: FROM, SELECT, WHERE, FUN

function (vas::VirtualACSet)(f::Function, args...; kwargs...)
    vas.view = f(vas, args...; kwargs...)
end

# get the number of rows
function ACSetInterface.nparts(vas::VirtualACSet{Conn}, table::Symbol)::DataFrame where Conn
    query = From(table) |> Group() |> Select(Agg.count())
    DBInterface.execute(vas.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.maxpart(vas::VirtualACSet, table::Symbol) 
    query = From(table) |> Group() |> Select(Agg.max(:_id))
    DBInterface.execute(vas.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.subpart(vas::VirtualACSet, table::Symbol)
    query = FROM(table) |> SELECT(*) 
    df = DBInterface.execute(vas.conn, query) |> DataFrames.DataFrame
    metadata!(df, "ob", table; style=:note)
    df
end

function tablefromcolumn(vas::VirtualACSet, column::Symbol)
    indices = map(values(vas.conn.catalog.tables)) do table
        haskey(table.columns, column)
    end
    collect(keys(vas.conn.catalog.tables))[findfirst(indices)]
end

function ACSetInterface.subpart(vas::VirtualACSet, ks::Vector{Int}, column::Symbol)
    table = tablefromcolumn(vas, column)
    query = FROM(table) |> WHERE(FUN("in", :_id, ks...)) |> SELECT(column)
    df = DBInterface.execute(vas.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.subpart(vas::VirtualACSet, key::Int, column::Symbol)
    subpart(vas, [key], column)
end

function ACSetInterface.subpart(vas::VirtualACSet, (:), column::Symbol)
    table = tablefromcolumn(vas, column)
    query = FROM(table) |> SELECT(column)
    df = DBInterface.execute(vas.conn, query) |> DataFrames.DataFrame
end
# TODO we can probably use DataFrames metadata to look at two dataframes as if we were looking at an ACSet. Maybe we can have a diagram of BasicSchema, say E ⇉ V where the values on these nodes are data frames

# incident

# TODO names::Vector{Symbol}
function ACSetInterface.incident(vas::VirtualACSet, ids::Vector{Int}, column::Symbol)
    table = tablefromcolumn(vas, column)
    query = FROM(table) |> WHERE(FUN("in", column, ids...)) |> SELECT(:_id)
    df = DBInterface.execute(vas.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.incident(vas::VirtualACSet, id::Int, column::Symbol)
    incident(vas, [id], column)
end

# function ACSetInterface.incident(vas::VirtualACSet, table::Symbol, names::AbstractVector{Symbol}) 
#     nst = namesrctgt(acset_schema(vas.acsettype()))
#     table = nst[names]
# end

# add_part!

function ACSetInterface.add_part!(vas::VirtualACSet, table::Symbol, values::Vector{<:NamedTuple{T}}) where T 
    execute!(vas, ACSetInsert(table, values))
end

function ACSetInterface.add_part!(vas::VirtualACSet, table, value::NamedTuple{T}) where T
    add_part!(vas, table, [value])
end

# set_subpart!

function ACSetInterface.set_subpart!(vas::VirtualACSet, 
        table::Symbol, values::Vector{<:NamedTuple{T}}; wheres::Union{WhereClause, Nothing}=nothing) where T 
    query = execute!(vas, ACSetUpdate(table, values, wheres))
    df = DataFrames.DataFrame(query); metadata!(df, "ob", table, style=:note)
    df
end

# clear_subpart!

function ACSetInterface.clear_subpart!(vas::VirtualACSet, args...) end

function ACSetInterface.rem_part!(vas::VirtualACSet, table::Symbol, id::Int)
    rem_parts!(vas, table, [id])
end

function ACSetInterface.rem_parts!(vas::VirtualACSet, table::Symbol, ids::Vector{Int}) 
    # if a table is constrained by another we might need to turn off foreign_key_checks
    execute!(vas, ACSetDelete(table, ids))
    reload!(vas)
    execute!(vas, ACSetSelect(table))
end

# rem_parts!(vas, :V, 6:11) foreign key issue
function ACSetInterface.rem_parts!(vas::VirtualACSet, table::Symbol, ids::UnitRange{Int64})
    rem_parts!(vas, table, collect(ids))
end

function ACSetInterface.cascading_rem_part!(vas::VirtualACSet, args...) end

#
function Schemas.objects(vas::VirtualACSet)
    execute!(vas, ShowTables())
end

function Schemas.homs(vas::VirtualACSet) end

function Schemas.arrows(vas::VirtualACSet) end
