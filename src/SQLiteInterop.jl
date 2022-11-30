module SQLiteInterop
  using AlgebraicRelations.Schemas
  using Catlab.CategoricalAlgebra
  using Tables
  using ...SQLite
  export db2schema

  function db2schema(db::SQLite.DB)
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