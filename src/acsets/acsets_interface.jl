#####################
### ACSets Interface
#####################

function (vas::VirtualACSet)(f::Function, args...; kwargs...)
    vas.view = f(vas, args...; kwargs...)
end

# get the number of rows
function ACSetInterface.nparts(acset::VirtualACSet{Conn}, table::Symbol)::DataFrame where Conn
    query = DBInterface.execute(acset.conn, "SELECT COUNT(*) FROM $table;")
    DataFrames.DataFrame(query) 
end

function ACSetInterface.maxpart(acset::VirtualACSet, table::Symbol) end

function ACSetInterface.subpart(vas::VirtualACSet, table::Symbol, select::Select)
    stmt = tostring(vas, select)
    query = DBInterface.execute(vas.conn, stmt)
    df = DataFrames.DataFrame(query); metadata!(df, "ob", table; style=:note)
    df
end

function ACSetInterface.subpart(vas::VirtualACSet, table::Symbol, what::SQLSelectQuantity=SelectAll())
    stmt = tostring(vas, Select(table; what=what))
    query = DBInterface.execute(vas.conn, stmt)
    df = DataFrames.DataFrame(query); metadata!(df, "ob", table; style=:note)
    df
end

function ACSetInterface.subpart(vas::VirtualACSet, key::Vector{Int}, column::Symbol)
    nst = namesrctgt(acset_schema(vas.acsettype()))
    table = nst[column] |> first
    select = Select(table, what=SelectColumns(table => :_id, table => column), 
                    wheres=WhereClause(:in, :_id => key))
    subpart(vas, table, select)
end

function ACSetInterface.subpart(vas::VirtualACSet, key::Int, column::Symbol)
    subpart(vas, [key], column)
end

function ACSetInterface.subpart(vas::VirtualACSet, (:), column::Symbol; what::SQLSelectQuantity=SelectAll())
    nst = namesrctgt(acset_schema(vas.acsettype()))
    table = nst[column].first
    subpart(vas, table, SelectColumns(table => column))
    # TODO I want a way of combining Select statements
end
# TODO we can probably use DataFrames metadata to look at two dataframes as if we were looking at an ACSet. Maybe we can have a diagram of BasicSchema, say E ⇉ V where the values on these nodes are data frames

# incident

# TODO names::Vector{Symbol}
function ACSetInterface.incident(vas::VirtualACSet, ids::Vector{Int}, name::Symbol)
    nst = namesrctgt(acset_schema(vas.acsettype()))
    table = nst[name]
    select = Select(table.first, what=SelectColumns(table.first => :_id),
                    wheres=WhereClause(:in, name => ids))
    subpart(vas, table.first, select)
end

# TODO names::Vector{Symbol}
function ACSetInterface.incident(vas::VirtualACSet, id::Int, name::Symbol)
    incident(vas, [id], name)
end

# function ACSetInterface.incident(vas::VirtualACSet, table::Symbol, names::AbstractVector{Symbol}) 
#     nst = namesrctgt(acset_schema(vas.acsettype()))
#     table = nst[names]
# end

# add_part!

function ACSetInterface.add_part!(vas::VirtualACSet, table::Symbol, values::Vector{<:NamedTuple{T}}) where T 
    stmt = tostring(vas, Insert(table, values))
    query = DBInterface.execute(vas, stmt)
    DBInterface.lastrowid(query)
end

function ACSetInterface.add_part!(vas::VirtualACSet, table, value::NamedTuple{T}) where T
    add_part!(vas, table, [value])
end

# set_subpart!

function ACSetInterface.set_subpart!(vas::VirtualACSet, 
        table::Symbol, values::Vector{<:NamedTuple{T}}; wheres::Union{WhereClause, Nothing}=nothing) where T 
    stmt = tostring(vas, Update(table, values, wheres))
    query = DBInterface.execute(vas.conn, stmt)
    df = DataFrames.DataFrame(query); metadata!(df, "ob", table, style=:note)
    df
end

# clear_subpart!

function ACSetInterface.clear_subpart!(acset::VirtualACSet, args...) end

function ACSetInterface.rem_part!(vas::VirtualACSet, table::Symbol, id::Int)
    rem_parts!(vas, table, [id])
end

function ACSetInterface.rem_parts!(vas::VirtualACSet, table::Symbol, ids::Vector{Int}) 
    # if a table is constrained by another we might need to turn off foreign_key_checks
    stmt = tostring(vas, Delete(table, ids))
    query = DBInterface.execute(vas.conn, stmt)
    result = tostring(vas, Select(table))
    query = DBInterface.execute(vas.conn, result)
    DataFrames.DataFrame(query)
end

# rem_parts!(vas, :V, 6:11) foreign key issue
function ACSetInterface.rem_parts!(vas::VirtualACSet, table::Symbol, ids::UnitRange{Int64})
    rem_parts!(vas, table, collect(ids))
end

function ACSetInterface.cascading_rem_part!(acset::VirtualACSet, args...) end
