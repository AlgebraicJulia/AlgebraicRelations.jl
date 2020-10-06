module ACSetQueries
  using Catlab: @present
  import Catlab.Programs.RelationalPrograms: TheoryTypedRelationDiagram
  import Catlab.Programs.RelationalPrograms: parse_relation_diagram
  using Catlab.Graphics
  using Catlab.WiringDiagrams
  using Catlab.CategoricalAlgebra.CSets
  using ..ACSetDB

  # Used for the redefinition of copy_parts!
  using Catlab.Theories: Schema, FreeSchema, dom, codom,
    CatDesc, CatDescType, AttrDesc, AttrDescType, SchemaType,
    ob_num, hom_num, data_num, attr_num, dom_num, codom_num

  export TheoryQuery, Query, @query, to_sql, draw_query

  const SQLOperators = Dict(:< => ("<", [:first, :second]),
                            :> => (">", [:first, :second]),
                            :(==) => ("=", [:first, :second]))

  @present TheoryQuery <: TheoryTypedRelationDiagram begin
    field::Attr(Port, Name)
  end

  const Query = ACSetType(TheoryQuery,
                          index=[:box, :junction, :outer_junction, :field],
                          unique_index=[:variable])

  Query() = Query{Symbol, Symbol, Symbol}()

  function Query(schema, wd)
    q = Query()
    copy_parts!(q, wd, (Junction=:, Box=:, OuterPort=:, Port=:))

    box_names = subpart(wd, :name)
    port_names = get_fields(schema)
    port_per_box = port_indices(wd)

    port_names = map(enumerate(subparts(wd, [:box, :junction, :port_type]))) do (i,p)
      box = p[1]
      junction = p[2]
      port_type = p[3]

      box_name = box_names[box]
      if box_name in keys(SQLOperators)
        SQLOperators[box_name][2][port_per_box[i]]
      else
        port_names[box_name][port_per_box[i]][1]
      end
    end
    set_subparts!(q, 1:nparts(q, :Port), field=port_names)
    q
  end

  macro query(schema, exprs...)
    Expr(:call, GlobalRef(ACSetQueries, :parse_query_statement),
                esc(schema), exprs)
  end

  function parse_query_statement(schema, exprs)
    wd = parse_relation_diagram((expr for expr in exprs)...)
    Query(schema, wd)
  end

  function port_indices(wd)
    box_sizes = zeros(Int, nparts(wd, :Box))
    map(subparts(wd, [:box])) do b
      box_sizes[b[1]] += 1
      return box_sizes[b[1]]
    end
  end

  # Generates a new array where all strings are unique. The new strings are based
  # off of the strings provided in the original array.
  uniquify(a::Array{String,1}) = begin
    a_n = Array{String,1}()
    # Fill a_n with unique values
    for i in 1:length(a)
      cur_val = a[i]

      # Iterate the digit until the value is unique
      while cur_val in a_n
        if occursin(r"_\d*$", cur_val)
          cur_val = replace(cur_val, r"_\d*$" => (c) -> "_"*string(parse(Int, c[2:end])+1))
        else
          cur_val = cur_val*"_0"
        end
      end
      append!(a_n, [cur_val])
    end
    return a_n
  end


  function to_sql(q::Query)

    box_names = subpart(q, :name)
    table_inds = findall(x -> !(x in keys(SQLOperators)), box_names)
    op_inds = findall(x -> x in keys(SQLOperators), box_names)

    outer_juncs = subpart(q, :outer_junction)
    port_info = subparts(q, [:box, :junction, :field])
    junctions = zeros(Int, nparts(q, :Junction))

    # Construct the conditions by iterating through each port
    conditions = Array{String,1}()
    for (i, p) in enumerate(port_info)
      if p[1] in table_inds
        prev_pind = junctions[p[2]]
        if prev_pind == 0
          junctions[p[2]] = i
        else
          p_port = port_info[prev_pind]
          push!(conditions, "t$(p[1]).$(p[3])=t$(p_port[1]).$(p_port[3])")
        end
      end
    end

    # Construct the operator relations
    append!(conditions, map(op_inds) do i
              p_inf = port_info[junctions[subpart(q, ports(q,i), :junction)]]
              op = SQLOperators[box_names[i]][1]
              "t$(p_inf[1][1]).$(p_inf[1][3])$(op)t$(p_inf[2][1]).$(p_inf[2][3])"
            end)

    # Construct the aliases for each table
    alias = map(table_inds) do i "$(box_names[i]) AS t$i" end

    # Construct the field output selection statement
    selector_alias = uniquify(map(i -> "$(port_info[junctions[i]][3])",outer_juncs))
    selectors = map(enumerate(outer_juncs)) do (i,j)
      port = port_info[junctions[j]]
      "t$(port[1]).$(port[3]) AS $(selector_alias[i])"
    end

    return "SELECT $(join(selectors, ", "))\nFROM $(join(alias, ", "))\nWHERE $(join(conditions, " AND "))"
  end

  function draw_query(q)
    to_graphviz(q; box_labels=:name, junction_labels=:junction_type,
                   port_labels=true, edge_attrs=Dict(:len => "0.8"))
  end

  # Replication of CSet functionality
  # TODO: Find best way to copy objects and attributes between CSets of
  #       different types
  function subparts(acs::ACSet, names::Array{Symbol,1})
    collect(zip([subpart(acs, name) for name in names]...))
  end

  function copy_parts!(acs::ACSet, from::ACSet, parts::NamedTuple{types}) where types
    parts = map(types, parts) do type, part
      part == (:) ? (1:nparts(from, type)) : part
    end
    _copy_parts!(acs, from, NamedTuple{types}(parts))
  end

  @generated function _copy_parts!(acs, from::T, parts::NamedTuple{types}) where
      {types,CD,AD,Ts,Idx,T <: ACSet{CD,AD,Ts,Idx}}
    obnums = ob_num.(CD, types)
    in_obs, out_homs = Symbol[], Tuple{Symbol,Symbol,Symbol}[]
    for (hom, dom, codom) in zip(CD.hom, CD.dom, CD.codom)
      if dom ∈ obnums && codom ∈ obnums
        push!(in_obs, CD.ob[codom])
        push!(out_homs, (hom, CD.ob[dom], CD.ob[codom]))
      end
    end
    in_obs = Tuple(unique!(in_obs))
    quote
      newparts = NamedTuple{$types}(tuple($(map(types) do type
        :(_copy_parts_data!(acs, from, Val($(QuoteNode(type))), parts.$type))
      end...)))
      partmaps = NamedTuple{$in_obs}(tuple($(map(in_obs) do type
        :(Dict{Int,Int}(zip(parts.$type, newparts.$type)))
      end...)))
      for (name, dom, codom) in $(Tuple(out_homs))
        for (p, newp) in zip(parts[dom], newparts[dom])
          q = subpart(from, p, name)
          newq = get(partmaps[codom], q, nothing)
          if !isnothing(newq)
            set_subpart!(acs, newp, name, newq)
          end
        end
      end
      newparts
    end
  end

  @generated function _copy_parts_data!(acs, from::T, ::Val{ob}, parts) where
      {CD,AD,T<:ACSet{CD,AD},ob}
    attrs = collect(filter(attr -> dom(AD, attr) == ob, AD.attr))
    quote
      newparts = add_parts!(acs, $(QuoteNode(ob)), length(parts))
      $(Expr(:block, map(attrs) do attr
         :(set_subpart!(acs, newparts, $(QuoteNode(attr)),
                        from.tables.$ob.$attr[parts]))
        end...))
      newparts
    end
  end
end
