module SQLiteAlgRelExt

using AlgebraicRelations
using Catlab.CategoricalAlgebra
using Tables
using SQLite

using FunSQL
using FunSQL: render, reflect
using MLStyle

function AlgebraicRelations.reload!(vas::VirtualACSet{SQLite.DB})
    conn = SQLite.DB()
    vas.conn = FunSQL.DB(conn, catalog=reflect(conn))
end

# DB specific, type conversion
tosql(::VirtualACSet{SQLite.DB}, ::Type{<:Real}) = "REAL"
tosql(::VirtualACSet{SQLite.DB}, ::Type{<:AbstractString}) = "TEXT"
tosql(::VirtualACSet{SQLite.DB}, ::Type{<:Symbol}) = "TEXT"
tosql(::VirtualACSet{SQLite.DB}, ::Type{<:Integer}) = "INTEGER"
tosql(::VirtualACSet{SQLite.DB}, T::DataType) = error("$T is not supported in this SQLite implementation")
# value conversion
tosql(::VirtualACSet{SQLite.DB}, ::Nothing) = "NULL"
tosql(::VirtualACSet{SQLite.DB}, x::T) where T<:Number = x
tosql(::VirtualACSet{SQLite.DB}, s::Symbol) = string(s)
tosql(::VirtualACSet{SQLite.DB}, s::String) = "\'$s\'"
tosql(::VirtualACSet{SQLite.DB}, x) = x

# TODO I don't like that the conversion function is also formatting. 
# I would be at peace if formatting and value representation were separated
function tosql(vas::VirtualACSet{SQLite.DB}, v::NamedTuple{T}; key::Bool=true) where T
    join(collect(Iterators.map(pairs(v)) do (k, v)
                     key ? "$(tosql(vas, k)) = $(tosql(vas, v))" : "$(tosql(vas, v))"
    end), ", ")
end

function tosql(vas::VirtualACSet{SQLite.DB}, values::Values{T}; key::Bool=true) where T
    if length(values.vals) == 1
        "$(tosql(vas, only(values.vals); key=key))"
    else
        join(["($x)" for x ∈ tosql.(Ref(vas), values.vals; key=key)], ", ")
    end
end

# String constructors
export render

function FunSQL.render(vas::VirtualACSet{SQLite.DB}, i::ACSetInsert)
    cols = join(columns(i.values), ", ")
    values = join(["($x)" for x ∈ tosql.(Ref(vas), i.values.vals; key=false)], ", ")
    "INSERT IGNORE INTO $(i.table) ($cols) VALUES $values ;"
end

function FunSQL.render(vas::VirtualACSet{SQLite.DB}, u::ACSetUpdate) 
    cols = join(columns(u.values), ", ")
    wheres = !isnothing(u.wheres) ? render(vas, u.wheres) : ""
    @info wheres
    "UPDATE $(u.table) SET $(tosql(vas, u.values)) " * wheres * ";"
end

# TODO might have to refactor so we can reuse code for show method
function FunSQL.render(vas::VirtualACSet{SQLite.DB}, s::ACSetSelect)
    from = s.from isa Vector ? join(s.from, ", ") : s.from
    qty = render(vas, s.qty)
    join = !isnothing(s.join) ? render(vas, s.join) : " "
    wheres = !isnothing(s.wheres) ? render(vas, s.wheres) : ""
    "SELECT $qty FROM $from " * join * wheres * ";"
end

function FunSQL.render(vas::VirtualACSet{SQLite.DB}, j::ACSetJoin)
    "$(j.type) JOIN $(j.table) ON $(render(vas, j.on))"
end

function FunSQL.render(vas::VirtualACSet{SQLite.DB}, ons::Vector{SQLEquation})
    join(render.(Ref(vas), ons), " AND ")
end

function FunSQL.render(vas::VirtualACSet{SQLite.DB}, eq::SQLEquation)
    "$(eq.lhs.first).$(eq.rhs.second) = $(eq.rhs.first).$(eq.rhs.second)"
end

function FunSQL.render(vas::VirtualACSet{SQLite.DB}, qty::SQLSelectQuantity)
    @match qty begin
        ::SelectAll || ::SelectDistinct || ::SelectDistinctRow => "*"
        SelectColumns(cols) => join(render.(Ref(vas), cols), ", ")
    end
end

function FunSQL.render(::VirtualACSet{SQLite.DB}, column::Union{Pair{Symbol, Symbol}, Symbol})
    @match column begin
        ::Pair{Symbol, Symbol} => "$(column.first).$(column.second)"
        _ => column
    end
end

# TODO
function FunSQL.render(::VirtualACSet{SQLite.DB}, wheres::WhereClause)
    @match wheres begin
        WhereClause(op, d::Pair) => "WHERE $(d.first) $op ($(join(d.second, ", ")))"
        _ => wheres
    end
end

function FunSQL.render(vas::VirtualACSet, c::ACSetCreate)
    create_stmts = map(objects(c.schema)) do ob
        obattrs = attrs(c.schema; from=ob)
        "CREATE TABLE IF NOT EXISTS $(ob)(" * 
            join(filter(!isempty, ["_id INTEGER PRIMARY KEY",
                # column_name column_type
                join(map(homs(c.schema; from=ob)) do (col, src, tgt)
                       tgttype = tosql(vas, Int)
                       "$(col) $tgttype"
                end, ", "),
                join(map(obattrs) do (col, _, type)
                    # FIXME
                    "$(col) $(tosql(vas, subpart_type(vas.acsettype(), type)))" 
               end, ", ")]), ", ") * ");"
    end
    join(create_stmts, " ")
end

function FunSQL.render(vas::VirtualACSet{SQLite.DB}, d::ACSetDelete)
    "DELETE FROM $(d.table) WHERE _id IN ($(join(d.ids, ",")))"
end

function FunSQL.render(::VirtualACSet{SQLite.DB}, v::Values)
    "VALUES " * join(entuple(v), ", ") * ";"
end

function FunSQL.render(::VirtualACSet{SQLite.DB}, a::ACSetAlter)
    "ALTER TABLE $(a.refdom) ADD CONSTRAINT fk_$(ref) FOREIGN KEY ($(a.ref)) REFERENCES $(a.refcodom)(_id); "
end

function FunSQL.render(::VirtualACSet{SQLite.DB}, fkc::ForeignKeyChecks)
    "SET FOREIGN_KEY_CHECKS = $(Int(fkc.bool)) ;"
end

function FunSQL.render(::VirtualACSet{SQLite.DB}, ::ShowTables)
    "SELECT name FROM sqlite_master WHERE type='table';"
end

# convenience
function AlgebraicRelations.ForeignKeyChecks(vas::VirtualACSet{SQLite.DB}, stmt::String)
    l, r = render.(Ref(conn), ForeignKeyChecks.([false, true]))
    wrap(stmt, l, r)
end

# overloading syntactical constructors 
function AlgebraicRelations.ACSetInsert(vas::VirtualACSet{SQLite.DB}, acset::ACSet)
    map(objects(acset_schema(acset))) do ob
        ACSetInsert(vas, acset, ob)
    end
end

function AlgebraicRelations.ACSetInsert(vas::VirtualACSet{SQLite.DB}, acset::ACSet, table::Symbol)
    cols = colnames(acset, table)
    vals = getrows(vas, acset, table)
    ACSetInsert(table, vals, nothing)
end



#####
function AlgebraicRelations.SQLSchema(db::SQLite.DB)
  sch = SQLSchema()
  tables = [t.name for t in SQLite.tables(db)]
  tab_ind = add_parts!(sch, :Table, length(tables), tname=tables)
  tab2ind = Dict([tables[t] => t for t in tab_ind])

  col2ind = Dict{String, Dict{String, Int64}}()
  for t in tables
    col2ind[t] = Dict{String, Int64}()
    cols = SQLite.columns(db, t)
    for c in 1:length(cols.cid)
      type = cols.type[c]
      if cols.pk[c] == 1
        type *= " PRIMARY KEY"
      end
      c_ind = add_part!(sch, :Column, table=tab2ind[t], cname=cols.name[c], type=type)
      col2ind[t][cols.name[c]] = c_ind
    end
  end
  for t in tables
    res = DBInterface.execute(db, "PRAGMA foreign_key_list($t);")
    for r in res
      add_part!(sch, :FK, from = col2ind[t][r.from],
                          to = col2ind[r.table][r.to])
    end
  end
  sch
end
end
