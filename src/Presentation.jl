module Presentation
export Schema, TypeToSql, sql
using Catlab
using Catlab.Doctrines
using Catlab.Present

TypeToSql = Dict(String => "text",
                 Int64 => "int",
                 Float64 => "float4")


struct Schema{T, U}
  types::Vector{T}
  relations::Vector{U}
end

sql(s::Schema) = begin

  # First make sql for data types
  primitives = map(s.types) do t

    # if our type is primitive, just use the SQL type
    if typeof(t.args[1][2]) <: DataType
      return "-- primitive type $(TypeToSql[t.args[1][2]]);"
    end

    # else construct a composite type
    f = (i,x) -> "$i $(TypeToSql[x])" # Converts primitive types to sql types

    components = t.args[1][2]
    fields = (f(k,components[k]) for k in keys(components)) |> x->join(x, ", ")
    "CREATE TYPE $(t.args[1][1]) as ($(fields));"
  end

  # for the relations in your presentation, you want to create tables
  tables = map(s.relations) do t

    # Iterate through the domain and codomain types
    fields = map(enumerate(t.args[2:end])) do (i, a)

      # For each domain/codomain, the type could be a monoidal product of
      # multiple types. We need to check for that and deal with it separately

      if typeof(a) <: Catlab.Doctrines.FreeBicategoryRelations.Ob{:otimes}
        names = t.args[1].fields[i]

        group_units = map(enumerate(a.args)) do (j, unit)

          name = names[j]
          if isa(unit.args[1][2], DataType)
            return " $name $(TypeToSql[unit.args[1][2]])"
          end

          unit_name = unit.args[1][1]
          " $name $unit_name"
        end |> xs -> join(xs,",")

        return group_units
      end

      # If we get here, then otimes was not used
      name = t.args[1].fields[i]

      # for primitive types, we can just include them in the table directly
      if isa(a.args[1][2], DataType)
        return " $name $(TypeToSql[a.args[1][2]])"
      end

      col = a.args[1][1]
      " $name $col"
    end |> xs-> join(xs, ",")
    "CREATE TABLE $(t.args[1].name) ($(fields));"
  end
  return primitives, tables
end
end
