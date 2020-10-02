module ACSetDB
  using Catlab: @present
	using Catlab.Present
  using Catlab.CategoricalAlgebra.CSets
  export TheorySQL, SchemaType, to_sql_schema, @present, get_fields

  @present TheorySQL(FreeSchema) begin
    Int64::Data
    Float64::Data
    String::Data
  end;

	function SchemaType(present::Presentation)
	  ACSetType(present){Int64, Float64, String}
	end

  const AbstractSQL = AbstractACSetType(TheorySQL)
 
	function generate_schema(schema::AbstractACSet)
	    queries = map(collect(get_fields(schema))) do (name, col)
	        cols = ["$n $t" for (n,t) in col]
	        "CREATE TABLE $name ( $(name)ID PRIMARY KEY, $(join(cols, ", ")))"
	    end
	    join(queries, ";\n")
	end

	function get_fields(schema::AbstractACSet)
	    fields = Dict{Symbol, Array{Tuple{Symbol, Type},1}}()
	    for (name, table) in pairs(schema.tables)
	        table_name = name
	        
	        # Get the column names and types
	        col_names, types = eltype(table).parameters
	        fields[table_name] = map(zip(col_names,types.parameters)) do (n,t)
	            (n, t)
	        end
	    end
	    fields
	end
end