module SQL
export sql, present_sql, to_sql
using Catlab.Doctrines, Catlab.Present, Catlab.WiringDiagrams
using AlgebraicRelations.QueryLib, AlgebraicRelations.SchemaLib
import AlgebraicRelations.SchemaLib: Schema

TypeToSql = Dict(String => "text",
                 Int64 => "int",
                 Float64 => "float4")

function to_sql(t)
  if t isa DataType
    return TypeToSql[t]
  end
  return t
end

function evaluate_ports(q::Query)
  wd = q.wd
  tables = q.tables
  aliases = Array{String,1}()
  port_val = [[Array{String,1}(), Array{String,1}()] for i in 1:(length(box_ids(wd))+2)]

  # Fill in this mapping for junction boxes
  junction_ids = filter(v -> box(wd,v) isa Junction, box_ids(wd))
  for id in junction_ids
    cur_box = box(wd, id)
    port_val[id][1] = fill("*", length(cur_box.input_ports))
    port_val[id][2] = fill("*", length(cur_box.output_ports))
  end

  # Fill in this mapping for I/O boxes
  port_val[1][2] = fill("*", length(wd.input_ports))
  port_val[2][1] = fill("*", length(wd.output_ports))
  append!(junction_ids, [1,2])

  # Fill in this mapping for all non-junction boxes
  rel_ids = filter(x -> !(x in junction_ids), box_ids(wd))
  for id in rel_ids
    cur_box = box(wd, id)
    is_dagger = false
    if cur_box isa BoxOp{:dagger}
      cur_box = cur_box.box
      is_dagger = true
    end
    box_name = cur_box.value

    # Push table name to the total join statement and assign an alias
    push!(aliases, "$box_name AS t$id")

    # Set the dom/codom field names
    port_val[id][1] = map(1:length(cur_box.input_ports)) do port
                        "t$id."*tables[box_name][1][port]
                      end
    port_val[id][2] = map(1:length(cur_box.output_ports)) do port
                        "t$id."*tables[box_name][2][port]
                      end
    if is_dagger
      tmp = port_val[id][1]
      port_val[id][1] = port_val[id][2]
      port_val[id][2] = tmp
    end
  end

  # Evaluate all junction boxes
  junction_edges = filter(e -> (e.source.box in junction_ids ||
                                e.target.box in junction_ids), wires(wd))
  fill_junc!(p::Array{Array{String,1},1}, val::String) = begin
    p[1] = fill(val, length(p[1]))
    p[2] = fill(val, length(p[2]))
  end
  # Each iteration will evaluate at least one more edge (if there are any left)
  for iter in 1:length(junction_edges)
    for e in junction_edges
      sb = e.source.box
      sp = e.source.port
      tb = e.target.box
      tp = e.target.port
      # Assign a value to a "*" if what it's connected to is defined
      if port_val[sb][2][sp] == "*" && port_val[tb][1][tp] != "*"
        port_val[sb][2][sp] = port_val[tb][1][tp]
        if sb > 2
          fill_junc!(port_val[sb], port_val[sb][2][sp])
        end
      elseif port_val[tb][1][tp] == "*" && port_val[sb][2][sp] != "*"
        port_val[tb][1][tp] = port_val[sb][2][sp]
        if tb > 2
          fill_junc!(port_val[tb], port_val[tb][1][tp])
        end
      end
    end
  end

  aliases, port_val
end


function sql(q::Query)::String

  tables = q.tables
  wd = q.wd

  rel_statement = Array{String,1}()

  # Define a mapping from port to field name
  # This will have the format port_val[box][dom/cod][port] = String
  join_statement, port_val = evaluate_ports(q)

  # At this point, all junctions are defined. Now we just have to loop through
  # them to assign equivalences
  for e in wires(wd)
    src = port_val[e.source.box][2][e.source.port]
    dst = port_val[e.target.box][1][e.target.port]

    if src == "*" || dst == "*"
      throw(ArgumentError("Wiring Diagram is insufficiently defined"))
    end

    if src != dst && !(dst*"="*src in rel_statement) &&
                     !(src*"="*dst in rel_statement)
      push!(rel_statement, src*"="*dst)
    end
  end

  # The only important junction nodes are the input/output nodes
  dom_array = port_val[1][2]
  codom_array = port_val[2][1]

  select = "SELECT "*join(vcat(dom_array, codom_array), ", ")*"\n"
  from = "FROM "*join(join_statement, ", ")*"\n"
  condition = ";"
  if length(condition) != 0
    condition = "WHERE "*join(rel_statement, " AND ")*";"
  end

  return select*from*condition
end


sql(types_dict, tables, schema) = begin
  primitives = map(collect(types_dict)) do (key,val)
    names = val[1]
    types = val[2]

    if length(names) == 0
      # In this case, it's just a primitive type
      return "-- primitive type $(to_sql(types[1]))"
    end

    fields = map(enumerate(names)) do (ind, name)
      return "$name $(to_sql(types[ind]))"
    end
    statement = "CREATE TYPE $key AS ($(join(fields,", ")))"
  end

  data_tables = map(collect(tables)) do (key,val)
    dom_names   = val[1]
    codom_names = val[2]
    hom = schema.generators_by_name[Symbol(key)]

    # Evaluate Dom
    fields = Array{String,1}()
    if length(dom_names) > 1
      f_types = hom.type_args[1].args
      for i in 1:length(dom_names)
        type = f_types[i].args[1]
        if length(types_dict[type][1]) == 0
          push!(fields, "$(dom_names[i]) $(to_sql(types_dict[type][2][1]))")
        else
          push!(fields, "$(dom_names[i]) $type")
        end
      end
    else
      type = hom.type_args[1].args[1]
      if length(types_dict[type][1]) == 0
        push!(fields, "$(dom_names[1]) $(to_sql(types_dict[type][2][1]))")
      else
        push!(fields, "$(dom_names[1]) $type")
      end
    end

    # Evaluate Codom
    if length(codom_names) > 1
      f_types = hom.type_args[2].args
      for i in 1:length(codom_names)
        type = f_types[i].args[1]
        if length(types_dict[type][1]) == 0
          push!(fields, "$(codom_names[i]) $(to_sql(types_dict[type][2][1]))")
        else
          push!(fields, "$(codom_names[i]) $type")
        end
      end
    else
      type = hom.type_args[2].args[1]
      if length(types_dict[type][1]) == 0
        push!(fields, "$(codom_names[1]) $(to_sql(types_dict[type][2][1]))")
      else
        push!(fields, "$(codom_names[1]) $type")
      end
    end

    "CREATE TABLE $key ($(join(fields, ", ")))"
  end

  "$(join(vcat(primitives,data_tables),";\n"));"
end


# Need to generate a wrapper call around this to insert parameters
# If the original query accepts a person's name (p_name) and returns
# their manager's name (m_name) and the person's salary (salary), then
# the wrapped query would look like this:
#
#
# PREPARE "12345" (text, text) AS
# SELECT t3.p_name, t4.m_name, t5.salary
# FROM names AS t3, manager AS t4, salary AS t5
# WHERE t3.person=t4.person AND t3.person=t5.person
# AND t3.p_name = ROW($1, $2);
present_sql(q::Query, uid::String)::String = begin
  types = q.types
  tables = q.tables
  wd = q.wd

  rel_statement = Array{String,1}()

  # Define a mapping from port to field name
  # This will have the format port_val[box][dom/cod][port] = String
  join_statement, port_val = evaluate_ports(q)

  # At this point, all junctions are defined. Now we just have to loop through
  # them to assign equivalences
  for e in wires(wd)
    src = port_val[e.source.box][2][e.source.port]
    dst = port_val[e.target.box][1][e.target.port]

    if src == "*" || dst == "*"
      throw(ArgumentError("Wiring Diagram is insufficiently defined"))
    end

    if src != dst && !(dst*"="*src in rel_statement) &&
                     !(src*"="*dst in rel_statement)
      push!(rel_statement, src*"="*dst)
    end
  end

  # The only important junction nodes are the input/output nodes
  dom_array = port_val[1][2]
  codom_array = port_val[2][1]

  # Extrapolate types from dom names
  type_arr = Array{String,1}()

  dom_types = wd.input_ports
  cur_sym = 1
  for (ind, val) in enumerate(dom_array)
    type = types[dom_types[ind]]

    if length(type[1]) == 0
      push!(type_arr, to_sql(type[2][1]))
      relation = "$val=\$$cur_sym"
      push!(rel_statement, relation)
      cur_sym += 1
    else
      sym_arr = Array{String,1}()
      for j_type in type[2]
        push!(type_arr, to_sql(j_type))
        push!(sym_arr, "\$$cur_sym")
        cur_sym += 1
      end
      relation = "$val=ROW($(join(sym_arr, ",")))"
      push!(rel_statement, relation)
    end
  end

  select = "SELECT "*join(vcat(dom_array, codom_array), ", ")*"\n"
  from = "FROM "*join(join_statement, ", ")*"\n"
  condition = "WHERE "*join(rel_statement, " AND ")

  res = "PREPARE \"$uid\" ($(join(type_arr,","))) AS\n$select$from$condition;"
  return res
end
end
