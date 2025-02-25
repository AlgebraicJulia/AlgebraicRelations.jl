using DBInterface
using ACSets

mutable struct Connection
    catalog::FunSQL.SQLCatalog
    acset::ACSet
end
export Connection

function Connection(; schema::Union{

ACSets.tables(c::Connection) = tables(c.acset)

struct DBConnection <: DBInterface.Connection
    conn::Connection
end

ACSets.tables(db::DBConnection) = tables(db.conn)

DBInterface.connect(DB{RawConnType},
                    args...;
                    schema = nothing,
                    dialect = nothing,
                    cache = 256,
                    kws...)

# DBInterface.connect(::Type{LibPQ.Connection}, args...; kws...) =
#     LibPQ.Connection(args...; kws...)

# DBInterface.prepare(conn::LibPQ.Connection, args...; kws...) =
#     LibPQ.prepare(conn, args...; kws...)

# DBInterface.execute(conn::Union{LibPQ.Connection, LibPQ.Statement}, args...; kws...) =
#     LibPQ.execute(conn, args...; kws...)

function DBInterface.connect(::Type{Connection}, args...; kws...) =
    Connection(args...; kws...)

function DBInterface.connect(acset::ACSet)
    presentation = Presentation(acset)
    schema = SQLSchema(presentation)
    tables = SQLTable(schema)
    catalog = FunSQL.SQLCatalog(values(tables)...)
    return DBConnection(Connection(catalog, acset))
end

function DBInterface.prepare(conn::DBConnection, args...; kwargs...)
    return prepare(conn.conn, args...; kwargs...)
end

function DBInterface.execute(conn::DBConnection, args...; kwargs...)
    return execute(conn.conn, args...; kwargs...)
end

function DBInterface.execute(conn::DBConnection, str::AbstractString; kwargs...)
    return execute(conn.conn, str; kwargs...)
end

function DBInterface.execute(conn::DBConnection, str::AbstractString, params; kwargs...)
    return execute(conn.conn, str, params; kwargs...)
end

function DBInterface.execute(stmt::Statement, args...; kwargs...)
    return execute(stmt, args...; kwargs...)
end

DBInterface.close!(conn::DBConnection) = close(conn.conn)
