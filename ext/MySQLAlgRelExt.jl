module MySQLAlgRelExt

using Catlab.CategoricalAlgebra
using AlgebraicRelations
using AlgebraicRelations.Schemas
import AlgebraicRelations: tosql

using FunSQL
using FunSQL: render, reflect
using MLStyle
using MySQL
using Tables

# function AlgebraicRelations.SQLSchema(db::SQLite.DB)
#     sch = SQLSchema{String}()
#     tables = [t.name for t in SQLite.tables(db)]
#     tab_ind = add_parts!(sch, :Table, length(tables), tname=tables)
#     tab2ind = Dict([tables[t] => t for t in tab_ind])
#     col2ind = Dict{String, Dict{String, Int64}}()
#     for t in tables
#       col2ind[t] = Dict{String, Int64}()
#       cols = SQLite.columns(db, t)
#       for c in 1:length(cols.cid)
#         type = cols.type[c]
#         if cols.pk[c] == 1
#           type *= " PRIMARY KEY"
#         end
#         c_ind = add_part!(sch, :Column, table=tab2ind[t], cname=cols.name[c], type=type)
#         col2ind[t][cols.name[c]] = c_ind
#       end
#     end
#     for t in tables
#       res = DBInterface.execute(db, "PRAGMA foreign_key_list($t);")
#       for r in res
#         add_part!(sch, :FK, from = col2ind[t][r.from],
#                             to = col2ind[r.table][r.to])
#       end
#     end
#     sch
# end

# need to pass in user config
function AlgebraicRelations.reload!(source::DBSource{MySQL.Connection})
    conn = DBInterface.connect(MySQL.Connection, "localhost", "mysql", db="acsets", 
                                   unix_socket="/var/run/mysqld/mysqld.sock")
    source.conn = FunSQL.DB(conn, catalog=reflect(conn))
end

function tosql end

# DB specific, type conversion
tosql(::DBSource{MySQL.Connection}, ::Type{<:Real}) = "REAL"
tosql(::DBSource{MySQL.Connection}, ::Type{<:AbstractString}) = "TEXT"
tosql(::DBSource{MySQL.Connection}, ::Type{<:Symbol}) = "TEXT"
tosql(::DBSource{MySQL.Connection}, ::Type{<:Integer}) = "INTEGER"
tosql(::DBSource{MySQL.Connection}, T::DataType) = error("$T is not supported in this MySQL implementation")
# value conversion
tosql(::DBSource{MySQL.Connection}, ::Nothing) = "NULL"
tosql(::DBSource{MySQL.Connection}, x::T) where T<:Number = x
tosql(::DBSource{MySQL.Connection}, s::Symbol) = string(s)
tosql(::DBSource{MySQL.Connection}, s::String) = "\'$s\'"
tosql(::DBSource{MySQL.Connection}, x) = x

# TODO I don't like that the conversion function is also formatting. 
# I would be at peace if formatting and value representation were separated
function tosql(source::DBSource{MySQL.Connection}, v::NamedTuple{T}; key::Bool=true) where T
    join(collect(Iterators.map(pairs(v)) do (k, v)
                     key ? "$(tosql(source, k)) = $(tosql(source, v))" : "$(tosql(source, v))"
    end), ", ")
end

function tosql(source::DBSource{MySQL.Connection}, values::Values{T}; key::Bool=true) where T
    if length(values.vals) == 1
        "$(tosql(source, only(values.vals); key=key))"
    else
        join(["($x)" for x ∈ tosql.(Ref(source), values.vals; key=key)], ", ")
    end
end

# String constructors
export render

function FunSQL.render(source::DBSource{MySQL.Connection}, i::ACSetInsert)
    cols = join(columns(i.values), ", ")
    values = join(["($x)" for x ∈ tosql.(Ref(source), i.values.vals; key=false)], ", ")
    "INSERT IGNORE INTO $(i.table) ($cols) VALUES $values ;"
end

function FunSQL.render(source::DBSource{MySQL.Connection}, u::ACSetUpdate) 
    cols = join(columns(u.values), ", ")
    wheres = !isnothing(u.wheres) ? render(source, u.wheres) : ""
    @info wheres
    "UPDATE $(u.table) SET $(tosql(source, u.values)) " * wheres * ";"
end

# TODO might have to refactor so we can reuse code for show method
function FunSQL.render(source::DBSource{MySQL.Connection}, s::ACSetSelect)
    from = s.from isa Vector ? join(s.from, ", ") : s.from
    qty = render(source, s.qty)
    join = !isnothing(s.join) ? render(source, s.join) : " "
    wheres = !isnothing(s.wheres) ? render(source, s.wheres) : ""
    "SELECT $qty FROM $from " * join * wheres * ";"
end

function FunSQL.render(source::DBSource{MySQL.Connection}, j::ACSetJoin)
    "$(j.type) JOIN $(j.table) ON $(render(source, j.on))"
end

function FunSQL.render(source::DBSource{MySQL.Connection}, ons::Vector{SQLEquation})
    join(render.(Ref(source), ons), " AND ")
end

function FunSQL.render(source::DBSource{MySQL.Connection}, eq::SQLEquation)
    "$(eq.lhs.first).$(eq.rhs.second) = $(eq.rhs.first).$(eq.rhs.second)"
end

function FunSQL.render(source::DBSource{MySQL.Connection}, qty::SQLSelectQuantity)
    @match qty begin
        ::SelectAll || ::SelectDistinct || ::SelectDistinctRow => "*"
        SelectColumns(cols) => join(render.(Ref(source), cols), ", ")
    end
end

function FunSQL.render(::DBSource{MySQL.Connection}, column::Union{Pair{Symbol, Symbol}, Symbol})
    @match column begin
        ::Pair{Symbol, Symbol} => "$(column.first).$(column.second)"
        _ => column
    end
end

# TODO
function FunSQL.render(::DBSource{MySQL.Connection}, wheres::WhereClause)
    @match wheres begin
        WhereClause(op, d::Pair) => "WHERE $(d.first) $op ($(join(d.second, ", ")))"
        _ => wheres
    end
end

function FunSQL.render(source::DBSource, c::ACSetCreate)
    create_stmts = map(objects(c.schema)) do ob
        obattrs = attrs(c.schema; from=ob)
        "CREATE TABLE IF NOT EXISTS $(ob)(" * 
            join(filter(!isempty, ["_id INTEGER PRIMARY KEY",
                # column_name column_type
                join(map(homs(c.schema; from=ob)) do (col, src, tgt)
                       tgttype = tosql(source, Int)
                       "$(col) $tgttype"
                end, ", "),
                join(map(obattrs) do (col, _, type)
                    # FIXME
                    "$(col) $(tosql(source, subpart_type(source.acsettype(), type)))" 
               end, ", ")]), ", ") * ");"
    end
    join(create_stmts, " ")
end

function FunSQL.render(source::DBSource{MySQL.Connection}, d::ACSetDelete)
    "DELETE FROM $(d.table) WHERE _id IN ($(join(d.ids, ",")))"
end

function FunSQL.render(::DBSource{MySQL.Connection}, v::Values)
    "VALUES " * join(entuple(v), ", ") * ";"
end

function FunSQL.render(::DBSource{MySQL.Connection}, a::ACSetAlter)
    "ALTER TABLE $(a.refdom) ADD CONSTRAINT fk_$(ref) FOREIGN KEY ($(a.ref)) REFERENCES $(a.refcodom)(_id); "
end

function FunSQL.render(::DBSource{MySQL.Connection}, ::ShowTables)
    "SHOW TABLES;"
end

function FunSQL.render(::DBSource{MySQL.Connection}, fkc::ForeignKeyChecks)
    "SET FOREIGN_KEY_CHECKS = $(Int(fkc.bool)) ;"
end

# convenience
function AlgebraicRelations.ForeignKeyChecks(source::DBSource{MySQL.Connection}, stmt::String)
    l, r = render.(Ref(conn), ForeignKeyChecks.([false, true]))
    wrap(stmt, l, r)
end

# overloading syntactical constructors 
function AlgebraicRelations.ACSetInsert(source::DBSource{MySQL.Connection}, acset::ACSet)
    map(objects(acset_schema(acset))) do ob
        ACSetInsert(source, acset, ob)
    end
end

function AlgebraicRelations.ACSetInsert(source::DBSource{MySQL.Connection}, acset::ACSet, table::Symbol)
    cols = colnames(acset, table)
    vals = getrows(source, acset, table)
    ACSetInsert(table, vals, nothing)
end

end
