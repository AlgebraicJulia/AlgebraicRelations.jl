# ####################
# # ACSets Interface
# ####################

# get the number of rows
function ACSetInterface.nparts(db::DBSource, table::Symbol)
    query = From(table) |> Group() |> Select(Agg.count())
    DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.maxpart(db::DBSource, table::Symbol) 
    query = From(table) |> Group() |> Select(Agg.max(:_id))
    DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.subpart(db::DBSource, table::Symbol)
    query = FROM(table) |> SELECT(*) 
    df = DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
    metadata!(df, "ob", table; style=:note)
    df
end

function tablefromcolumn(db::DBSource, column::Symbol)
    indices = map(values(db.conn.catalog.tables)) do table
        haskey(table.columns, column)
    end
    !isempty(indices) || return nothing
    collect(keys(db.conn.catalog.tables))[findfirst(indices)]
end

function ACSetInterface.subpart(db::DBSource, ks::Vector{Int}, column::Symbol)
    table = tablefromcolumn(db, column)
    !isempty(table) || return nothing
    query = FROM(table) |> WHERE(FUN("in", :_id, ks...)) |> SELECT(column)
    df = DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.subpart(db::DBSource, key::Int, column::Symbol)
    subpart(db, [key], column)
end

function ACSetInterface.subpart(db::DBSource, (:), tablecolumn::Pair{Symbol, Symbol})
    query = FROM(tablecolumn.first) |> SELECT(tablecolumn.second)
    df = DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.subpart(db::DBSource, (:), column::Symbol)
    query = FROM(table) |> SELECT(column)
    df = DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
end
# TODO we can probably use DataFrames metadata to look at two dataframes as if we were looking at an ACSet. Maybe we can have a diagram of BasicSchema, say E ⇉ V where the values on these nodes are data frames

# incident

function ACSetInterface.incident(db::DBSource, vals::Vector, tablecolumn::Pair{Symbol, Symbol})
    query = FROM(tablecolumn.first) |> WHERE(FUN(:in, tablecolumn.second, vals...)) |> SELECT(:_id)
    # query = From(tablecolumn.first) |> Select(:_id) TODO document why this is not permitted
    df = DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.incident(db::DBSource, val::Symbol, tablecolumn::Pair{Symbol, Symbol})
    incident(db, [val], tablecolumn)
end

# TODO names::Vector{Symbol}
function ACSetInterface.incident(db::DBSource, vals::Vector, column::Symbol)
    table = tablefromcolumn(db, column)
    query = From(table) |> Where(Fun.in(Get(column), vals...)) |> Select(:_id)
    df = DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
end

function ACSetInterface.incident(db::DBSource, val::Symbol, column::Symbol)
    incident(db, [val], column)
end

# function ACSetInterface.incident(db::DBSource, table::Symbol, names::AbstractVector{Symbol}) 
#     nst = namesrctgt(acset_schema(db.acsettype()))
#     table = nst[names]
# end

# add_part!

function ACSetInterface.add_part!(db::DBSource, table::Symbol, values::Vector{<:NamedTuple{T}}) where T 
    execute!(db, ACSetInsert(table, values))
end

function ACSetInterface.add_part!(db::DBSource, table, value::NamedTuple{T}) where T
    add_part!(db, table, [value])
end

# set_subpart!

function ACSetInterface.set_subpart!(db::DBSource, 
        table::Symbol, values::Vector{<:NamedTuple{T}}; wheres::Union{WhereClause, Nothing}=nothing) where T 
    query = execute!(db, ACSetUpdate(table, values, wheres))
    df = DataFrames.DataFrame(query); metadata!(df, "ob", table, style=:note)
    df
end

# clear_subpart!

function ACSetInterface.clear_subpart!(db::DBSource, args...) end

function ACSetInterface.rem_part!(db::DBSource, table::Symbol, id::Int)
    rem_parts!(db, table, [id])
end

function ACSetInterface.rem_parts!(db::DBSource, table::Symbol, ids::Vector{Int}) 
    # if a table is constrained by another we might need to turn off foreign_key_checks
    execute!(db, ACSetDelete(table, ids))
    reload!(db)
    execute!(db, ACSetSelect(table))
end

# rem_parts!(db, :V, 6:11) foreign key issue
function ACSetInterface.rem_parts!(db::DBSource, table::Symbol, ids::UnitRange{Int64})
    rem_parts!(db, table, collect(ids))
end

function ACSetInterface.cascading_rem_part!(db::DBSource, args...) end

#
function Schemas.objects(db::DBSource)
    execute!(db, ShowTables())
end

# foreign keys
function Schemas.homs(db::DBSource) end

# columns
function Schemas.arrows(db::DBSource) end
