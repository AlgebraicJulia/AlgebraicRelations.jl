module SQLiteAlgRelExt

using AlgebraicRelations
using AlgebraicRelations: SQL
import AlgebraicRelations.Fabric
import AlgebraicRelations: sql

using Catlab.CategoricalAlgebra

using TraitInterfaces

using Tables
using SQLite
using MLStyle

function AlgebraicRelations.reload!(source::DBSource{SQLite.DB})
    conn = SQLite.DB()
    source.conn = FunSQL.DB(conn, catalog=FunSQL.reflect(conn))
end

# TODO move to render
function create(source::DBSource{SQLite.DB}, t::ACSet)
    s = acset_schema(t)
    stmts = map(objects(s)) do ob
        obattrs = attrs(s; from=ob)
        "CREATE TABLE IF NOT EXISTS $ob(" *
        join(filter(!isempty, ["_id INTEGER PRIMARY KEY",
            join(map(homs(s; from=ob)) do (col, _, _)
                tgttype = sql(source, Int)
                "$col $tgttype"
            end, ", "),
            join(map(obattrs) do (col, src, tgt)
                "$col $(sql(source, subpart_type(t, tgt)))"
            end, ", ")
        ]), ", ")
    end
    join(stmts, " ")
end
export create

# DB specific, type conversion
# TODO confusing mess of type and value conversion
sql(::DBSource{SQLite.DB}, ::Type{<:Real}) = "REAL"
sql(::DBSource{SQLite.DB}, ::Type{<:AbstractString}) = "TEXT"
sql(::DBSource{SQLite.DB}, ::Type{<:AbstractChar}) = "TEXT"
sql(::DBSource{SQLite.DB}, ::Type{<:Symbol}) = "TEXT"
sql(::DBSource{SQLite.DB}, ::Type{<:Integer}) = "INTEGER"
sql(::DBSource{SQLite.DB}, fk::FK{T}) where T = fk.val
sql(::DBSource{SQLite.DB}, s::Type{FK{T}}) where T = "INTEGER"
sql(::DBSource{SQLite.DB}, T::DataType) = error("$T is not supported in this SQLite implementation")
# _value conversion
sql(::DBSource{SQLite.DB}, ::Nothing) = "NULL"
sql(::DBSource{SQLite.DB}, x::T) where T<:Number = x
sql(::DBSource{SQLite.DB}, s::Symbol) = "\'$(string(s))\'"
sql(::DBSource{SQLite.DB}, s::String) = "\'$s\'"
# sql(::DBSource{SQLite.DB}, x) = x

function sql(::DBSource{SQLite.DB}, t)
    f = @λ begin
        ::Type{<:Real} => "REAL"
        ::Type{<:Integer} => "INTEGER"
        ::Type{<:FK} => "INTEGER" # TODO foreign key
        ::Type{<:Union{AbstractString, Char, Symbol}} => "TEXT"
        ::Nothing => "NULL"
        s::Symbol || ::AbstractString => "\'$s\'"
        fk::FK{T} where T => f(fk.val)
        s => s 
    end
    f(t)
end

function AlgebraicRelations.Fabric.from_sql(::DBSource{SQLite.DB}, s::String)
    @match s begin
        "INT" || "int" || "INTEGER" => Integer
        "TEXT" || "varchar(255)" => String
        _ => Any
    end
end

# TODO I don't like that the conversion function is also formatting. 
# I would be at peace if formatting and value representation were separated
function sql(source::DBSource{SQLite.DB}, v::NamedTuple{T}; key::Bool=true) where T
    join(collect(Iterators.map(pairs(v)) do (k, v)
        key ? "$(sql(source, k)) = $(sql(source, v))" : "$(sql(source, v))"
    end), ", ")
end

# TODO syntax
function sql(source::DBSource{SQLite.DB}, values::Values{T}; key::Bool=true) where T
    if length(values.vals) == 1
        "$(sql(source, only(values.vals); key=key))"
    else
        join(["($x)" for x ∈ sql.(Ref(source), values.vals; key=key)], ", ")
    end
end

include("render.jl")

# convenience
function AlgebraicRelations.ForeignKeyChecks(source::DBSource{SQLite.DB}, stmt::String)
    l, r = render.(Ref(conn), ForeignKeyChecks.([false, true]))
    wrap(stmt, l, r)
end

# overloading syntactical constructors 
function AlgebraicRelations.Insert(source::DBSource{SQLite.DB}, acset::ACSet)
    [Insert(source, acset, ob) for ob in acset_schema(acset)]
end

function AlgebraicRelations.Insert(source::DBSource{SQLite.DB}, acset::ACSet, table::Symbol)
    cols = colnames(acset, table)
    vals = getrows(source, acset, table)
    Insert(table, vals, nothing)
end

function AlgebraicRelations.SQL.get_schema(source::DBSource{SQLite.DB})
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
    τ = trait(source)
    AlgebraicRelations.execute![τ](source, cmd)
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
