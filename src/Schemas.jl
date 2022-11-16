module Schemas
  using Catlab.CategoricalAlgebra
  using Catlab.Present
  using FunSQL: SQLTable

  export pres2schema, render_schema, SQLSchema

  """ Schema for an SQL Schema
  This ACSet schema contains the relevant information for an SQL schema. This is
  visualized in q.uiver [here](https://q.uiver.app/?q=WzAsNSxbMCwxLCJUYWJsZSJdLFswLDAsIk5hbWUiXSxbMiwwLCJUeXBlIl0sWzIsMSwiQ29sdW1uIl0sWzIsMiwiRksiXSxbNCwzLCJ0byIsMCx7Im9mZnNldCI6LTJ9XSxbNCwzLCJmcm9tIiwyLHsib2Zmc2V0IjoyfV0sWzMsMiwidHlwZSIsMl0sWzMsMCwidGFibGUiLDFdLFswLDEsInRuYW1lIl0sWzIsMSwidHluYW1lIiwxXSxbMywxLCJjbmFtZSIsMV1d)
  """
  @present TheorySQLSchema(FreeSchema) begin
    Name::AttrType

    (Table, Column, FK)::Ob

    (to, from)::Hom(FK, Column)
    table::Hom(Column, Table)

    tname::Attr(Table, Name)
    (type, cname)::Attr(Column, Name)
  end;


  @abstract_acset_type AbstractSQLSchema
  @acset_type SQLSchema(TheorySQLSchema) <: AbstractSQLSchema

  TypeToSQL = Dict("String" => "TEXT",
                 "Int" => "INTEGER",
                 "Int64" => "INTEGER",
                 "IntArray" => "INTEGER[]",
                 "FloatMatrix" => "INTEGER[][]",
                 "Float64" => "REAL",
                 "FloatArray" => "REAL[]",
                 "FloatMatrix" => "REAL[][]",
                 "Bool" => "BOOLEAN",
                 "Date" => "DATE")

  type2sql(::Type{<:Number}) = "REAL"
  type2sql(::Type{<:Vector{<:Number}}) = "REAL[]"
  type2sql(::Type{<:Matrix{<:Number}}) = "REAL[][]"
  type2sql(::Type{<:Int}) = "INTEGER"
  type2sql(::Type{<:Vector{<:Int}}) = "INTEGER[]"
  type2sql(::Type{<:Matrix{<:Int}}) = "INTEGER[][]"
  type2sql(::Type{<:String}) = "TEXT"
  type2sql(s::Symbol) = type2sql("$s")
  type2sql(s::String) = s ∈ keys(TypeToSQL) ? TypeToSQL[s] : "TEXT"


  SQLSchema(args...) = SQLSchema{String}(args...)

  function pres2schema(p::Presentation; types::Union{Dict, Nothing}=nothing)
    fields = get_fields(p, types)
    sch = SQLSchema()
    tables = keys(fields)
    id = "SERIAL PRIMARY KEY"
    fk = "INTEGER"

    tab2ind = Dict{Symbol, Int64}()
    for t in tables
      t_ind = add_part!(sch, :Table, tname="$t")
      add_part!(sch, :Column, table=t_ind, cname="id", type=id)
      tab2ind[t] = t_ind
    end
    for t in tables
      for c in fields[t]
        if c[1] == :Hom
          col = add_part!(sch, :Column, table = tab2ind[t], cname = "$(c[3])", type=fk)
          add_part!(sch, :FK, from=col, to=tab2ind[c[2]])
        else
          type = type2sql(c[2])
          add_part!(sch, :Column, table = tab2ind[t], cname = "$(c[3])", type=type)
        end
      end
    end
    sch
  end

  # Targets of foreign keys are not included in the final ACSet
  # Does not adequately address the problem of multiple foreign keys between
  # tables
  function schema2pres(sch::SQLSchema)
    pres = Presentation(FreeSchema)
    ob_map = Dict([o => Ob(FreeSchema, Symbol(o)) for o in sch[:tname]])
    attr_map = Dict([t => AttrType(FreeSchema.AttrType, t) for t in unique(sch[:type])])
    homs = map(parts(sch, :FK)) do fk
      fc, tc = sch[fk, :from], sch[fk, :to]
      ft, tt = sch[[fc, tc], :table]
      Hom(Symbol(ob_map[sch[ft, :tname]], "!", sch[fc, :cname]),
                 ob_map[sch[ft, :tname]], ob_map[sch[tt, :tname]])
    end
    attr_cols = filter(c -> isempty(vcat(incident(sch, c, :to), incident(sch, c, :from))),
                       parts(sch, :Column))
    attrs = map(attr_cols) do c
      cname = sch[c, :cname]
      type = sch[c, :type]
      tname = sch[sch[c, :table], :tname]
      Attr(Symbol(tname, "!", cname),
                 ob_map[tname], attr_map[type])
    end
  end

  function to_json(sch::Presentation)
    fields = get_fields(sch)
    Dict(map(collect(keys(fields))) do k
             k => vcat([f[2] for f in fields[k]], [:id])
    end)
  end

  # Just remove all info prior to `!` (this helps with ensuring generated ACSets have unique field names)
  function hom_name(generator)
    name = first(generator.args)
    Symbol(last(split("$name", "!")))
  end
  function ob_name(generator)
    name = generator.args[1]
    Symbol(last(split("$name", "!")))
  end

  function get_fields(sch::Presentation, types::Union{Dict, Nothing})
    fields = Dict{Symbol, Vector}()
    for obj in sch.generators[:Ob]
      fields[ob_name(obj)] = Vector{Any}()
    end
    for h in sch.generators[:Hom]
        fname = hom_name(h)
        if !isnothing(fname)
            push!(fields[ob_name(dom(h))], (:Hom, ob_name(codom(h)), Symbol(fname)))
        end
    end
    for a in sch.generators[:Attr]
        fname = hom_name(a)
        if !isnothing(fname)
          if !isnothing(types)
            push!(fields[ob_name(dom(a))], (:Attr, types[codom(a).args[1]], Symbol(fname)))
          else
            push!(fields[ob_name(dom(a))], (:Attr, codom(a).args[1], Symbol(fname)))
          end
        end
    end
    fields
  end

  function render_schema(sch::SQLSchema)
    table_gen = map(1:nparts(sch, :Table)) do t
        cols = incident(sch, t, :table)
        filter!(c -> isempty(incident(sch, c, :from)), cols)
        col_gen = ["$(sch[c, :cname]) $(sch[c, :type])" for c in cols]

        "CREATE TABLE IF NOT EXISTS $(sch[t, :tname])($(join(col_gen, ", ")))"
    end
    fk_gen = map(1:nparts(sch, :FK)) do fk
      from_col = sch[fk, :from]
      from_tab = sch[from_col, :table]
      to_col = sch[fk, :to]
      to_tab = sch[to_col, :table]
      join(("ALTER TABLE $(sch[from_tab, :tname])",
            "ADD COLUMN $(sch[from_col, :cname]) INTEGER",
            "REFERENCES $(sch[to_tab, :tname])($(sch[to_col, :cname]))"), " ")
    end
    "$(join(vcat(table_gen, fk_gen), ";\n"));"
  end


  function load_schema(filename::String)
    load_schema(JSON.parsefile(filename))
  end
  function load_schema(dict::Dict)
    Dict{Symbol, Union{SQLTable, SQLNode}}(map(collect(keys(dict))) do k
      Symbol(k) => SQLTable(Symbol(k), columns = Symbol.(dict[k]))
    end)
  end
end
