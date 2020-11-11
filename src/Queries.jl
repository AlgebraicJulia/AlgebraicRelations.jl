module Queries
  using Catlab: @present
  import Catlab.Programs.RelationalPrograms: TheoryTypedRelationDiagram
  import Catlab.Programs.RelationalPrograms: parse_relation_diagram
  using Catlab.Programs.RelationalPrograms
  using Catlab.Graphics
  using Catlab.WiringDiagrams
  using Catlab.CategoricalAlgebra.CSets
  using Catlab.CategoricalAlgebra
  using Catlab.CategoricalAlgebra.FinSets
  using ..DB

  # Used for the redefinition of copy_parts!
  using Catlab.Theories: Schema, FreeSchema, dom, codom,
    CatDesc, CatDescType, AttrDesc, AttrDescType, SchemaType,
    ob_num, hom_num, data_num, attr_num, dom_num, codom_num

  export TheoryQuery, Query, @query, to_sql, draw_query, to_prepared_sql, infer!

  const SQLOperators = Dict(:<    => ("<", [:first, :second]),
                            :>    => (">", [:first, :second]),
                            :(==) => ("=", [:first, :second]),
                            :<=   => ("<=", [:first, :second]),
                            :>=   => (">=", [:first, :second]),
                            :(!=) => ("<>", [:first, :second]),
                           )

  @present TheoryQuery <: TheoryTypedRelationDiagram begin
    field::Attr(Port, Name)
    Comparison::Ob
    comp_port1::Hom(Comparison, Port)
    comp_port2::Hom(Comparison, Port)
    # comp_port1⋅port_type == comp_port2⋅port_type
    # port_type == junction⋅junction_type
    # subpart(q, :port_type) == subpart(q, subpart(q, :junction), :junction_type)
  end



  const Query = ACSetType(TheoryQuery,
                          index=[:box, :junction, :outer_junction, :field, :variable])

  const OpenQueryOb, OpenQuery = OpenACSetTypes(Query, :Junction)

  NullableSym = Union{Symbol, Nothing}
  Query() = Query{NullableSym, NullableSym, NullableSym}()

  """    Open(dynam::DynamUWD, bundling=nothing)

  This function converts a closed dynamic system to an open dynamical system
  (structured multicospan). If a `bundling` is not provided, then every outer
  port will have its own cospan.

  """
  function Open(query_orig::Query, bundling=nothing)
    query = deepcopy(query_orig)
    cur_jncs = nparts(query, :Junction)
    cur_ports = nparts(query, :OuterPort)

    # Create cospan legs for each outer_junction
    legs = map(i -> FinFunction([subpart(query, i, :outer_junction)], cur_jncs), 1:cur_ports)

    # The cospans are now keeping track of composition, so we remove redundant
    # information stored in the outerports (this is restored in the closeDynam
    # function
    rem_parts!(query, :OuterPort, 1:nparts(query, :OuterPort))

    # Create the OpenDynam object
    op_query = OpenQuery{NullableSym, NullableSym, NullableSym}(query, legs...)

    # Bundle legs if `bundling` provided
    if !isnothing(bundling)
      op_query = bundle_legs(op_query, bundling)
    end
    op_query
  end

  function infer!(wd, rels::Array{Tuple{Array{Symbol,1}, Array{Symbol,1}},1}; max_iter=2*length(rels))
    # Perform multiple steps to fill in chains of relations
    for i in 1:max_iter
      inf_step = [infer_indiv!(wd, rel) for rel in rels]
      if all([!s[2] for s in inf_step])
        if all([s[1] for s in inf_step])
          # Successfully defined all values to be defined
          return true
        end
        # Did not define all values, but stopped changing
        throw(ErrorException("Not all relations were able to be defined. Original system insufficiently defined"))
      end
    end
    return false
  end

  function infer_indiv!(wd, rel::Tuple{Array{Symbol}, Array{Symbol}})
    a = subpart(wd, rel[1][1])

    for i in 2:length(rel[1])
      if i == length(rel[1])
        a = view(subpart(wd, rel[1][i]), a)
      else
        a = subpart(wd, a, rel[1][i])
      end
    end

    b = subpart(wd, rel[2][1])
    for i in 2:length(rel[2])
      if i == length(rel[2])
        b = view(subpart(wd, rel[2][i]), b)
      else
        b = subpart(wd, b, rel[2][i])
      end
    end
    changed = false
    length(a) == length(b) || throw(DimensionMismatch("The arguments of $(rel[1])==$(rel[2]) have different lengths, $(length(a)) and $(length(b))"))
    is_defined = map(1:length(a)) do i
      if a[i] == nothing
        if b[i] == nothing
          return false
        else
          a[i] = b[i]
          changed = true
        end
      else
        if b[i] == nothing
          b[i] = a[i]
          changed = true
        else
          a[i] == b[i] || throw(DimensionMismatch("The arguments of $(rel[1])==$(rel[2]) have inconsistent values at index $i, $(a[i]) and $(b[i])"))
        end
      end
      true
    end
    return all(is_defined), changed
  end

  function RelToQuery(rel, schema)

    transform = schema_to_dict(schema)
    q = oapply(rel, transform)

    set_subpart!(q.cospan.apex, :variable, subpart(rel, :variable))
    add_parts!(q.cospan.apex, :OuterPort, nparts(rel, :OuterPort),
                                          outer_junction=subpart(rel, :outer_junction),
                                          outer_port_type=nothing)
    infer!(q.cospan.apex, [([:port_type],[:junction, :junction_type]),
               ([:outer_junction, :junction_type],[:outer_port_type]),
               ([:comp_port1,:port_type],[:comp_port2,:port_type])]);

    q.cospan.apex
  end



  function Query(schema, wd)
    RelToQuery(wd, schema)
  end

  macro query(schema, exprs...)
    Expr(:call, GlobalRef(Queries, :parse_query_statement),
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
    variables = subpart(q, :variable)
    prepared_junctions = findall(x -> string(x)[1] == '_', variables)

    junctions[prepared_junctions] = nparts(q, :Port) .+ (1:length(prepared_junctions))

    field_name(port) = begin
      if port > length(port_info)
        "\$$(port - length(port_info))"
      else
        p = port_info[port]
        "t$(p[1]).$(p[3])"
      end
    end

    # Construct the conditions by iterating through each port
    conditions = Array{String,1}()
    for (i, p) in enumerate(port_info)
      if p[1] in table_inds
        prev_pind = junctions[p[2]]
        if prev_pind == 0
          junctions[p[2]] = i
        else
          push!(conditions, "$(field_name(i))=$(field_name(prev_pind))")
        end
      end
    end

    # Construct the operator relations
    append!(conditions, map(op_inds) do i
              p_ind = junctions[subpart(q, incident(q,i,:box), :junction)]
              op = SQLOperators[box_names[i]][1]
              "$(field_name(p_ind[1]))$(op)$(field_name(p_ind[2]))"
            end)

    # Construct the aliases for each table
    alias = map(table_inds) do i "$(box_names[i]) AS t$i" end

    # Construct the field output selection statement
    selector_alias = uniquify(map(i -> "$(variables[i])",outer_juncs))
    selectors = map(enumerate(outer_juncs)) do (i,j)
      "$(field_name(junctions[j])) AS $(selector_alias[i])"
    end

    query = "SELECT $(join(selectors, ", "))\nFROM $(join(alias, ", "))"

    if length(conditions) > 0
      query = string(query, "\nWHERE $(join(conditions, " AND "))")
    end

    query
  end

  function to_prepared_sql(q::Query, uid::String)
    junc_types = map(x -> typeToSQL(x), subpart(q, :junction_type))
    prepared_junctions = findall(x -> string(x)[1] == '_', subpart(q, :variable))

    return "PREPARE \"$uid\" ($(join(junc_types[prepared_junctions], ","))) AS\n$(to_sql(q))",
           length(prepared_junctions)
  end

  function draw_query(q; kw...)
    uwd = TypedRelationDiagram{NullableSym, NullableSym, NullableSym}()
    copy_parts!(uwd, q)
    to_graphviz(uwd; box_labels=:name, junction_labels=:variable, kw...)
  end

  function schema_to_dict(schema)
    port_names = get_fields(schema)

    sym_to_q = Dict{Symbol, OpenQuery}()

    for name in keys(SQLOperators)
      new_q = Query()
      add_parts!(new_q, :Junction, 2, junction_type=nothing, variable=nothing)
      add_part!(new_q, :Box, name=name)
      add_parts!(new_q, :Port, 2, box=1, junction=[1,2],
                 field=SQLOperators[name][2], port_type=[nothing, nothing])
      add_part!(new_q, :Comparison, comp_port1=1, comp_port2=2)
      add_parts!(new_q, :OuterPort, 2, outer_junction=[1,2], outer_port_type=nothing)
      sym_to_q[name] = Open(new_q)
    end

    for name in keys(port_names)
      fields = first.(port_names[name])
      types = Symbol.(getindex.(port_names[name], 2))

      new_q = Query()
      add_part!(new_q, :Box, name=name)
      add_parts!(new_q, :Junction, length(fields), junction_type=nothing, variable=nothing)
      add_parts!(new_q, :Port, length(fields), box=1,
                                               junction=1:length(fields),
                                               field=fields,
                                               port_type=types)
      add_parts!(new_q, :OuterPort, length(fields), outer_junction=1:length(fields),
                                                   outer_port_type=nothing)
      sym_to_q[name] = Open(new_q)
    end
    sym_to_q
  end

  # Replication of CSet functionality
  # TODO: Find best way to copy objects and attributes between CSets of
  #       different types
  function subparts(acs::ACSet, names::Array{Symbol,1})
    collect(zip([subpart(acs, name) for name in names]...))
  end
end
