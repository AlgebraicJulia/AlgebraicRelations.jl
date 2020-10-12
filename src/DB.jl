module DB
  using Catlab: @present
  using Catlab.Present
  using Catlab.CategoricalAlgebra.CSets
  export TheorySQL, SchemaType, generate_schema_sql, @present, get_fields, TypeToSQL, typeToSQL

  TypeToSQL = Dict("String" => "text",
                   "Int64" => "int",
                   "Float64" => "float4")

  typeToSQL(x) = TypeToSQL[string(x)]
  @present TheorySQL(FreeSchema) begin
    Int64::Data
    Float64::Data
    String::Data
  end;

  function SchemaType(present::Presentation)
    ACSetType(present){Int64, Float64, String}
  end

  const AbstractSQL = AbstractACSetType(TheorySQL)

  function generate_schema_sql(schema::AbstractACSet)
    queries = map(collect(get_fields(schema))) do (name, col)
      cols = ["$n $(typeToSQL(t))" for (n,t) in col]
      "CREATE TABLE $name ($(join(cols, ", ")))"
    end
    string(join(queries, ";\n"), ";")
  end

  function get_fields(schema::AbstractACSet)
    fields = Dict{Symbol, Array{Tuple{Symbol, Type},1}}()
    for (name, table) in pairs(schema.tables)
      table_name = name

      # Get the column names and types
      col_names, types = eltype(table).parameters
      col_names = map(x -> Symbol(split(string(x), r"_\d+_")[end]), col_names)
      fields[table_name] = map(zip(col_names,types.parameters)) do (n,t)
        (n, t)
      end
    end
    fields
  end
end
