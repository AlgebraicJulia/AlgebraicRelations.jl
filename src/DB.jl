module DB
  using Catlab: @present
  using Catlab.Present
  using Catlab.CategoricalAlgebra.CSets
  export TheorySQL, SchemaType, generate_schema_sql, @present, get_fields, TypeToSQL, typeToSQL

  TypeToSQL = Dict("String" => "text",
                   "Int" => "int",
                   "Int64" => "int",
                   "Real" => "real",
                   "Bool" => "boolean")

  typeToSQL(x) = TypeToSQL[string(x)]
  @present TheorySQL(FreeSchema) begin
    Int::Data
    Int64::Data
    Real::Data
    String::Data
    Bool::Data
  end;

  function SchemaType(present::Presentation)
    ACSetType(present){Int, Int64, Real, String, Bool}
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

  # TODO: Maybe add in a way to get_fields from a Presentation?
  #       Allows more convenience when working with workflows,
  #       but doesn't allow you to take advantage of the CSet
  #       structure
end
