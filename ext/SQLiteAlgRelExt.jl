module SQLiteAlgRelExt

using AlgebraicRelations
using Catlab.CategoricalAlgebra
using Tables
using SQLite

using FunSQL
using FunSQL: render, reflect
using MLStyle

function AlgebraicRelations.reload!(vas::VirtualACSet{SQLite.Connection})
    conn = SQLite.DB()
    vas.conn = FunSQL.DB(conn, catalog=reflect(conn))
end

# DB specific, type conversion
tosql(::VirtualACSet{SQLite.Connection}, ::Type{<:Real}) = "REAL"
tosql(::VirtualACSet{SQLite.Connection}, ::Type{<:AbstractString}) = "TEXT"
tosql(::VirtualACSet{SQLite.Connection}, ::Type{<:Symbol}) = "TEXT"
tosql(::VirtualACSet{SQLite.Connection}, ::Type{<:Integer}) = "INTEGER"
tosql(::VirtualACSet{SQLite.Connection}, T::DataType) = error("$T is not supported in this MySQL implementation")
# value conversion
tosql(::VirtualACSet{SQLite.Connection}, ::Nothing) = "NULL"
tosql(::VirtualACSet{SQLite.Connection}, x::T) where T<:Number = x
tosql(::VirtualACSet{SQLite.Connection}, s::Symbol) = string(s)
tosql(::VirtualACSet{SQLite.Connection}, s::String) = "\'$s\'"
tosql(::VirtualACSet{SQLite.Connection}, x) = x



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
