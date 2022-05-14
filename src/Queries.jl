module Queries
  export to_funsql, to_tables

  using Catlab, Catlab.CategoricalAlgebra
  using JSON
  using FunSQL: SQLTable, Where, Join, Select, Get, render, From, As, Fun, SQLNode

  using ..Schemas

  function to_tables(sch::SQLSchema)
    Dict{Symbol, Union{SQLTable, SQLNode}}(map(parts(sch, :Table)) do t
      tname = Symbol(lowercase(sch[t, :tname]))
      tname => SQLTable(tname, columns = Symbol.(sch[incident(sch, t, :table),:cname]))
    end)
  end


  function to_funsql(rel, sch::SQLSchema; queries::Dict{Symbol, SQLNode} = Dict{Symbol, SQLNode}())
    to_funsql(rel, merge(to_tables(sch), queries))
  end

  function to_funsql(rel, schema::Dict{Symbol, Union{SQLTable, SQLNode}})
    included_tables = fill(false, nparts(rel, :Box))
    box_uid = [Symbol("b$i") for i in 1:nparts(rel, :Box)]
    j_value = Vector{Any}([nothing for i in 1:nparts(rel, :Junction)])
    to_include = [1]
    rel[:name] .= Symbol.(lowercase.(string.(rel[:name])))

    funsql = schema[rel[1, :name]] isa SQLNode ?
                (schema[rel[1, :name]] |> As(box_uid[1])) :
                (From(schema[rel[1, :name]]) |> As(box_uid[1]))
    while(!isempty(to_include))
      cur_box = pop!(to_include)
      included_tables[cur_box] = true
      ports = incident(rel, cur_box, :box)
      join_rels = Vector{Any}()
      for p in ports
        jctn = rel[p, :junction]
        get_p = Get[box_uid[cur_box]][rel[p,:port_name]]
        if isnothing(j_value[jctn])
          j_value[jctn] = get_p
          for ip in incident(rel, jctn, :junction)
            ib = rel[ip, :box]
            if !(ib ∈ to_include || included_tables[ib])
              push!(to_include, ib)
            end
          end
        else
          push!(join_rels, j_value[jctn] .== get_p)
        end
      end
      if cur_box != 1
        funsql = Join(box_uid[cur_box] => schema[rel[cur_box, :name]];
                      on = isempty(join_rels) ? true : Fun.and(join_rels...))(funsql)
      end
    end
    funsql |> Select(map(j -> rel[j, :outer_port_name]=>j_value[rel[j, :outer_junction]], 1:nparts(rel, :OuterPort))...)
  end

#=
  function to_prepared_sql(q::Query, uid::String)
    junc_types = map(x -> typeToSQL(x), subpart(q, :junction_type))
    prepared_junctions = findall(x -> string(x)[1] == '_', subpart(q, :variable))

    return "PREPARE \"$uid\" ($(join(junc_types[prepared_junctions], ","))) AS\n$(to_sql(q))",
           length(prepared_junctions)
  end

  function draw_query(q; kw...)
    uwd = TypedNamedRelationDiagram{NullableSym, NullableSym, NullableSym}()
    copy_parts!(uwd, q)
    to_graphviz(uwd; box_labels=:name, junction_labels=:variable, kw...)
  end
  =#
end
