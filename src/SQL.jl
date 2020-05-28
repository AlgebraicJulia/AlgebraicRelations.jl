module SQL
export sql, TypeToSql
using Catlab.Doctrines, Catlab.Present, Catlab.WiringDiagrams
using AlgebraicRelations.QueryLib, AlgebraicRelations.Presentation
import AlgebraicRelations.Presentation: Schema

TypeToSql = Dict(String => "text",
                 Int64 => "int",
                 Float64 => "float4")

struct Vertex
  name::Symbol
  is_dagger::Bool
  is_junc::Bool
end

function sql(q::Query)::String

  ind_to_alias(i) = "t$i"
  wd = q.wd
  tables = q.tables

  # Get the vertices as (name, is_dagger, is_junc)
  verts = fill(Vertex(:*,false,false),nboxes(wd)+2)
  verts[1] = Vertex(:input,false,true)
  verts[2] = Vertex(:output,false,true)

  for k in box_ids(wd)
    v = box(wd, k)

    is_junc   = false
    is_dagger = false
    name      = :*
    if typeof(v) <: Junction
      is_junc = true
    elseif typeof(v) <: BoxOp{:dagger}
      is_dagger = true
      name = v.box.value
    else
      name = v.value
    end
    verts[k] = Vertex(name, is_dagger, is_junc)
  end

  # Make the join statement
  alias_array = Array{String,1}()
  for v in box_ids(wd)
    cur_b = verts[v]
    name = string(cur_b.name)
    if !cur_b.is_junc
      push!(alias_array, "$name AS $(ind_to_alias(v))")
    end
  end

  # Make the relation statement
  relation_array = Array{String,1}()

  # Neighbors will keep track of connections to junction nodes
  neighbors = fill((Array{String,1}(),Array{String,1}()), length(verts))

  neighbors[1] = (Array{String,1}(),fill("", length(wd.input_ports)))
  neighbors[2] = (fill("", length(wd.output_ports)),Array{String,1}())
  for i in 3:length(neighbors)
    if verts[i].is_junc
      neighbors[i] = ([""],[""])
    else
      neighbors[i] = (fill("",length(tables[verts[i].name][1])),
                      fill("",length(tables[verts[i].name][2])))
    end
  end

  for e in wires(wd)
    sb = e.source.box
    sp = e.source.port
    db = e.target.box
    dp = e.target.port
    src = ""
    dst = ""

    if verts[sb].is_junc && verts[db].is_junc
      continue
    elseif verts[sb].is_junc
      df = tables[verts[db].name][1][dp]
      if verts[db].is_dagger
        df = tables[verts[db].name][2][dp]
      end
      if neighbors[sb][2][sp] == ""
        neighbors[sb][2][sp] = "$(ind_to_alias(db)).$df"
        continue
      else
        src = neighbors[sb][2][sp]
        dst = "$(ind_to_alias(db)).$df"
      end
    elseif verts[db].is_junc
      sf = tables[verts[sb].name][2][sp]
      if verts[sb].is_dagger
        sf = tables[verts[sb].name][1][sp]
      end
      if neighbors[db][1][dp] == ""
        neighbors[db][1][dp] = "$(ind_to_alias(sb)).$sf"
        continue
      else
        src = neighbors[db][1][dp]
        dst = "$(ind_to_alias(sb)).$sf"
      end
    else
      sf = tables[verts[sb].name][2][sp]
      if verts[sb].is_dagger
        sf = tables[verts[sb].name][1][sp]
      end
      src = "$(ind_to_alias(sb)).$sf"

      df = tables[verts[db].name][1][dp]
      if verts[db].is_dagger
        df = tables[verts[db].name][2][dp]
      end
      dst = "$(ind_to_alias(db)).$df"
    end

    if dst*"="*src in relation_array || src*"="*dst in relation_array
      continue
    end
    push!(relation_array, src*"="*dst)
  end

  # The only important junction nodes are the input/output nodes
  dom_array = neighbors[1][2]
  codom_array = neighbors[2][1]

  select = "SELECT "*join(vcat(dom_array, codom_array), ", ")*"\n"
  from = "FROM "*join(alias_array, ", ")*"\n"
  condition = "WHERE "*join(relation_array, " AND ")*";"

  return select*from*condition
end

sql(types, tables, schema) = begin
  primitives = map(collect(types)) do (key,val)
    @show key
    names = val[1]
    types = val[2]

    if length(names) == 0
      # In this case, it's just a primitive type
      return "-- primitive type $(TypeToSql[types[1]])"
    end

    fields = map(enumerate(names)) do (ind, name)
      return "$name $(TypeToSql[types[ind]])"
    end
    statement = "CREATE TYPE $key AS ($(join(fields,", ")))"
  end

  data_tables = map(collect(tables)) do (key,val)
    @show key
    dom_names   = val[1]
    codom_names = val[2]
    hom = schema.generators_by_name[Symbol(key)]

    # Evaluate Dom
    fields = Array{String,1}()
    if length(dom_names) > 1
      f_types = hom.type_args[1].args
      for i in 1:length(dom_names)
        type = f_types[i].args[1]
        push!(fields, "$(dom_names[i]) $type")
      end
    else
      type = hom.type_args[1].args[1]
      push!(fields, "$(dom_names[1]) $type")
    end
    
    # Evaluate Codom
    if length(codom_names) > 1
      f_types = hom.type_args[2].args
      for i in 1:length(codom_names)
        type = f_types[i].args[1]
        push!(fields, "$(codom_names[i]) $type")
      end
    else
      type = hom.type_args[2].args[1]
      push!(fields, "$(codom_names[1]) $type")
    end
    
    "CREATE TABLE $key ($(join(fields, ", ")))"
  end

  "$(join(vcat(primitives,data_tables),";\n"));"
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
