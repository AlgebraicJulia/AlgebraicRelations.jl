module DB
  using Catlab: @present
  using Catlab.Present
  using Catlab.CategoricalAlgebra.CSets
  using Catlab.CSetDataStructures.DenseACSets: struct_acset
  export TheorySQL, generate_schema_sql, @present, get_fields, TypeToSQL, typeToSQL, @db_schema, AbstractSQL

  TypeToSQL = Dict("String" => "text",
                   "Int" => "int",
                   "Int64" => "int",
                   "Real" => "real",
                   "Bool" => "boolean")

  typeToSQL(x) = TypeToSQL[string(x)]
  @present TheorySQL(FreeSchema) begin
    Int::AttrType
    Int64::AttrType
    Real::AttrType
    String::AttrType
    Bool::AttrType
  end;

  @abstract_acset_type AbstractSQL

  # TODO: This should be replacable with a cleaner method
  macro db_schema(head)
    struct_name = gensym()
    quote
      $(esc(:eval))(struct_acset($(Meta.quot(struct_name)), AbstractSQL, $(esc(head.args[2]))))
      $(esc(head.args[1]))() = $(esc(struct_name)){$(esc(Int)), $(esc(Int)), $(esc(Real)), $(esc(String)), $(esc(Bool))}()
    end
  end

  function generate_schema_sql(schema::AbstractSQL)
    queries = map(collect(get_fields(schema))) do (name, col)
      cols = ["$n $(typeToSQL(t))" for (n,t) in col]
      "CREATE TABLE $name ($(join(cols, ", ")))"
    end
    string(join(queries, ";\n"), ";")
  end

  function get_fields(schema::AbstractSQL)
    fields = Dict{Symbol, Array{Tuple{Symbol, Type},1}}()
    for (name, table) in pairs(tables(schema))
      table_name = name

      # Get the column names and types
      col_names = propertynames(table)
      types = eltype.([schema[c] for c in col_names])
      col_names = map(x -> Symbol(split(string(x), r"_\d+_")[end]), col_names)
      fields[table_name] = map(zip(col_names,types)) do (n,t)
        (n, t)
      end
    end
    fields
  end

  # TODO: Maybe add in a way to get_fields from a Presentation?
  #       Allows more convenience when working with workflows,
  #       but doesn't allow you to take advantage of the CSet
  #       structure
end
