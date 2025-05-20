module SQLiteAlgRelExt

using AlgebraicRelations
import AlgebraicRelations.SQLACSets.Fabric

using Catlab.CategoricalAlgebra

using Tables
using SQLite
using MLStyle

function AlgebraicRelations.reload!(source::DBSource{SQLite.DB})
    conn = SQLite.DB()
    source.conn = FunSQL.DB(conn, catalog=reflect(conn))
end

# TODO move to render
function create(source::DBSource{SQLite.DB}, t::ACSet)
    s = acset_schema(t)
    stmts = map(objects(s)) do ob
        obattrs = attrs(s; from=ob)
        "CREATE TABLE IF NOT EXISTS $ob(" *
        join(filter(!isempty, ["_id INTEGER PRIMARY KEY",
            join(map(homs(s; from=ob)) do (col, _, _)
                tgttype = to_sql(source, Int)
                "$col $tgttype"
            end, ", "),
            join(map(obattrs) do (col, src, tgt)
                "$col $(to_sql(source, subpart_type(t, tgt)))"
            end, ", ")
        ]), ", ")
    end
    join(stmts, " ")
end
export create

# DB specific, type conversion
# to_sql(::DBSource{SQLite.DB}, ::Type{<:Real}) = "REAL"
# to_sql(::DBSource{SQLite.DB}, ::Type{<:AbstractString}) = "TEXT"
# to_sql(::DBSource{SQLite.DB}, ::Type{<:Symbol}) = "TEXT"
# to_sql(::DBSource{SQLite.DB}, ::Type{<:Integer}) = "INTEGER"
# to_sql(::DBSource{SQLite.DB}, T::DataType) = error("$T is not supported in this SQLite implementation")
# # _value conversion
# to_sql(::DBSource{SQLite.DB}, ::Nothing) = "NULL"
# to_sql(::DBSource{SQLite.DB}, x::T) where T<:Number = x
# to_sql(::DBSource{SQLite.DB}, s::Symbol) = string(s)
# to_sql(::DBSource{SQLite.DB}, s::String) = "\'$s\'"
# to_sql(::DBSource{SQLite.DB}, x) = x

function AlgebraicRelations.SQLACSets.Fabric.to_sql(::DBSource{SQLite.DB}, t)
    @match t begin
        ::Type{<:Real} => "REAL"
        ::Type{<:Integer} => "INTEGER"
        ::Type{<:T} where T <: Union{AbstractString, Char, Symbol} => "TEXT"
        ::Nothing => "NULL"
        ::Symbol => string(t)
        ::String => "\'$t\'"
        _ => "TEXT"
    end
end

function AlgebraicRelations.SQLACSets.Fabric.from_sql(::DBSource{SQLite.DB}, s::String)
    @match s begin
        "INT" || "int" || "INTEGER" => Integer
        "TEXT" || "varchar(255)" => String
        _ => Any
    end
end

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

function AlgebraicRelations.SQLACSets.Fabric.get_schema(source::DBSource{SQLite.DB})
    cmd = """
    SELECT 
        m.name AS table_name,
        p.cid AS column_id,
        p.name AS column_name,
        p.type AS data_type,
        p.dflt_value AS default_value,
        p.pk AS is_primary_key
    FROM     
        sqlite_master m
    JOIN     
        pragma_table_info(m.name) p
    WHERE 
        m.type = 'table' AND
        m.name NOT LIKE 'sqlite_%'
    ORDER BY 
        m.name,
        p.cid;
    """
    execute!(source, cmd)
end
export get_schema

# cmd ="SELECT m.name AS table_name,
# p.cid AS column_id,
# p.name AS column_name,
# p.type AS



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
