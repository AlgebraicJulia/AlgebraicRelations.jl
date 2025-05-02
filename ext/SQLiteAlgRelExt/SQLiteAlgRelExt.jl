module SQLiteAlgRelExt

using AlgebraicRelations
using Catlab.CategoricalAlgebra

using Tables
using SQLite
using MLStyle

function AlgebraicRelations.reload!(source::DBSource{SQLite.DB})
    conn = SQLite.DB()
    source.conn = FunSQL.DB(conn, catalog=reflect(conn))
end

# DB specific, type conversion
tosql(::DBSource{SQLite.DB}, ::Type{<:Real}) = "REAL"
tosql(::DBSource{SQLite.DB}, ::Type{<:AbstractString}) = "TEXT"
tosql(::DBSource{SQLite.DB}, ::Type{<:Symbol}) = "TEXT"
tosql(::DBSource{SQLite.DB}, ::Type{<:Integer}) = "INTEGER"
tosql(::DBSource{SQLite.DB}, T::DataType) = error("$T is not supported in this SQLite implementation")
# value conversion
tosql(::DBSource{SQLite.DB}, ::Nothing) = "NULL"
tosql(::DBSource{SQLite.DB}, x::T) where T<:Number = x
tosql(::DBSource{SQLite.DB}, s::Symbol) = string(s)
tosql(::DBSource{SQLite.DB}, s::String) = "\'$s\'"
tosql(::DBSource{SQLite.DB}, x) = x

# TODO I don't like that the conversion function is also formatting. 
# I would be at peace if formatting and value representation were separated
function tosql(source::DBSource{SQLite.DB}, v::NamedTuple{T}; key::Bool=true) where T
    join(collect(Iterators.map(pairs(v)) do (k, v)
        key ? "$(tosql(source, k)) = $(tosql(source, v))" : "$(tosql(source, v))"
    end), ", ")
end

function tosql(source::DBSource{SQLite.DB}, values::Values{T}; key::Bool=true) where T
    if length(values.vals) == 1
        "$(tosql(source, only(values.vals); key=key))"
    else
        join(["($x)" for x ∈ tosql.(Ref(source), values.vals; key=key)], ", ")
    end
end

include("render.jl")

# convenience
function AlgebraicRelations.ForeignKeyChecks(source::DBSource{SQLite.DB}, stmt::String)
    l, r = render.(Ref(conn), ForeignKeyChecks.([false, true]))
    wrap(stmt, l, r)
end

# overloading syntactical constructors 
function AlgebraicRelations.ACSetInsert(source::DBSource{SQLite.DB}, acset::ACSet)
    map(objects(acset_schema(acset))) do ob
        ACSetInsert(source, acset, ob)
    end
end

function AlgebraicRelations.ACSetInsert(source::DBSource{SQLite.DB}, acset::ACSet, table::Symbol)
    cols = colnames(acset, table)
    vals = getrows(source, acset, table)
    ACSetInsert(table, vals, nothing)
end

"""

This constructs a SQLSchema
"""
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
