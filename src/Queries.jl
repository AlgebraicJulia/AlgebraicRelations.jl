module Queries
  using Catlab: @present
  import Catlab.Programs.RelationalPrograms: UntypedNamedRelationDiagram
  import Catlab.Programs.RelationalPrograms: parse_relation_diagram
  using Catlab.Programs.RelationalPrograms
  using Catlab.Present
  using Catlab.Graphics
  using Catlab.WiringDiagrams
  using Catlab.CategoricalAlgebra.CSets
  using Catlab.CategoricalAlgebra
  using Catlab.CategoricalAlgebra.FinSets
  using Base.Iterators
  using ..DB
  using ..Presentations

  export TheoryQuery, Query, @query, to_sql, draw_query, to_prepared_sql, infer!, Schema, add_relation!

  const SQLOperators = Dict(:<    => ("<", [:first, :second]),
                            :>    => (">", [:first, :second]),
                            :(==) => ("=", [:first, :second]),
                            :<=   => ("<=", [:first, :second]),
                            :>=   => (">=", [:first, :second]),
                            :(!=) => ("<>", [:first, :second]),
                           )

  @present TheoryQuery(FreeSchema) begin
    Table::Ob
    Column::Ob
    Var::Ob

    Type::Data
    Name::Data

    table::Hom(Column, Table)
    relation::Hom(Column, Var)
    col_name::Attr(Column, Name)
    col_type::Attr(Column, Type)
    var_type::Attr(Var, Type)
    tab_name::Attr(Table, Name)

    Comparison::Ob
    comp_port1::Hom(Comparison, Column)
    comp_port2::Hom(Comparison, Column)

    Input::Ob
    input_var::Hom(Input, Var)
    # comp_port1⋅col_type == comp_port2⋅col_type
    # col_type == relation⋅var_type
    # subpart(q, :col_type) == subpart(q, subpart(q, :relation), :var_type)
  end



  const Query = ACSetType(TheoryQuery,
                          index=[:table, :relation, :col_name])

  const OpenQueryOb, OpenQuery = OpenACSetTypes(Query, :Var)

  struct NamedQuery
    query::OpenQuery
    labels::Array{Symbol,1}
  end

  """    NamedQuery(q::Query, labels::Array{Symbol,1}, bundles=nothing)

  This function converts a query to a named open query. If a `bundles` is not
  provided, then every junction will have its own cospan.

  """
  # TODO: Add bundling for NamedQuery constructor
  function NamedQuery(q::Query, labels::Array{Symbol,1}; bundles=nothing)
    cur_jncs = nparts(q, :Var)

    # Create cospan legs for each outer_junction
    legs = map(i -> FinFunction([i], cur_jncs), 1:cur_jncs)

    # Create the OpenDynam object
    op_query = OpenQuery{NullableSym, NullableSym}(q, legs...)

    # Bundle legs if `bundles` provided
    if !isnothing(bundles)
      op_query = bundle_legs(op_query, bundles)
      length(bundles) == length(labels) ||
          error("$(length(labels)) labels not equal to $(length(bundles)) bundles")
    else
      length(labels) == cur_jncs ||
          error("$(length(labels)) labels not equal to $(cur_jncs) junctions")
    end
    NamedQuery(op_query, labels)
  end

  struct Schema
    relations::Dict{Symbol, NamedQuery}
  end

  function Schema(presentation::Presentation)
    # TODO: It's probably easier just to get the schema dictionary straight
    # from the Presentation, but I'm wanting to preserve the step of taking it
    # into an ACSet for future work.

    sch = present_to_schema(presentation)
    Schema(schema_to_dict(sch()))
  end

  function clean_interface!(nq::NamedQuery)
    outer_juncs = collect(flatten([acfunc.components[:Var].func for acfunc in nq.query.cospan.legs]))
    set_subpart!(nq.query.cospan.apex, :var_type, nothing)
  end

  function add_relation!(sch::Schema; kw...)
    isempty(intersect(keys(sch.relations), keys(kw))) || throw(KeyError(intersect(keys(sch.relations), keys(kw))))

    for rel in values(kw)
      clean_interface!(rel)
    end
    merge!(sch.relations, kw)
  end

  NullableSym = Union{Symbol, Nothing}
  Query() = Query{NullableSym, NullableSym}()

#=  function Open(query_orig::Query, bundles=nothing)
    query = deepcopy(query_orig)
    cur_jncs = nparts(query, :Var)
    cur_ports = nparts(query, :OuterPort)

    # Create cospan legs for each outer_junction
    legs = map(i -> FinFunction([subpart(query, i, :outer_junction)], cur_jncs), 1:cur_ports)

    # The cospans are now keeping track of composition, so we remove redundant
    # information stored in the outerports (this is restored in the closeDynam
    # function
    rem_parts!(query, :OuterPort, 1:nparts(query, :OuterPort))

    # Create the OpenDynam object
    op_query = OpenQuery{NullableSym, NullableSym, NullableSym}(query, legs...)

    # Bundle legs if `bundles` provided
    if !isnothing(bundles)
      op_query = bundle_legs(op_query, bundles)
    end
    op_query
  end=#

  function Query(schema::Schema, wd)
    RelToQuery(wd, schema)
  end

  macro query(schema, exprs...)
    Expr(:call, GlobalRef(Queries, :parse_query_statement),
                esc(schema), exprs)
  end

  function parse_query_statement(schema::Schema, exprs)
    wd = parse_relation_diagram((expr for expr in exprs)...)
    Query(schema, wd)
  end

  function port_indices(wd)
    table_sizes = zeros(Int, nparts(wd, :Table))
    map(subparts(wd, [:table])) do b
      table_sizes[b[1]] += 1
      return table_sizes[b[1]]
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


  function to_sql(nq::NamedQuery)
    q = nq.query.cospan.apex
    labels = nq.labels
    legs = nq.query.cospan.legs

    table_names = subpart(q, :tab_name)
    table_inds = findall(x -> !(x in keys(SQLOperators)), table_names)
    op_inds = findall(x -> x in keys(SQLOperators), table_names)

    outer_juncs = flatten([acfunc.components[:Var].func for acfunc in legs])
    port_info = subparts(q, [:table, :relation, :col_name])
    junctions = zeros(Int, nparts(q, :Var))

    variables = collect(flatten(map(enumerate(legs)) do (ind, acfunc)
                          func = acfunc.components[:Var]
                          if length(func.func) > 1
                            [Symbol("$(string(lables[ind]))_$i") for i in func.func]
                          else
                            [labels[ind]]
                          end
                        end))

    prepared_junctions = subpart(q, :input_var)
    junctions[prepared_junctions] = nparts(q, :Column) .+ (1:length(prepared_junctions))

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
              p_ind = junctions[subpart(q, incident(q,i,:table), :relation)]
              op = SQLOperators[table_names[i]][1]
              "$(field_name(p_ind[1]))$(op)$(field_name(p_ind[2]))"
            end)

    # Construct the aliases for each table
    alias = map(table_inds) do i "$(table_names[i]) AS t$i" end

    # Construct the field output selection statement
    selectors = map(enumerate(outer_juncs)) do (i,j)
      "$(field_name(junctions[j])) AS $(variables[i])"
    end

    query = "SELECT $(join(selectors, ", "))\nFROM $(join(alias, ", "))"

    if length(conditions) > 0
      query = string(query, "\nWHERE $(join(conditions, " AND "))")
    end

    query
  end

  function to_prepared_sql(nq::NamedQuery, uid::String)
    q = nq.query.cospan.apex
    junc_types = map(x -> typeToSQL(x), subpart(q, :var_type))
    prepared_junctions = subpart(q, :input_var)

    return "PREPARE \"$uid\" ($(join(junc_types[prepared_junctions], ","))) AS\n$(to_sql(nq))",
           length(prepared_junctions)
  end

  function draw_query(q; kw...)
    labels = q.labels
    query = q.query.cospan.apex
    uwd = TypedRelationDiagram{NullableSym, NullableSym, NullableSym}()
    add_parts!(uwd, :Junction, nparts(query, :Var), junction_type=subpart(query, :var_type))
    add_parts!(uwd, :Box, nparts(query, :Table), name=subpart(query, :tab_name))
    add_parts!(uwd, :Port, nparts(query, :Column), box=subpart(query, :table),
                                                   junction=subpart(query, :relation),
                                                   port_type=subpart(query, :col_name))

    to_graphviz(uwd; box_labels=:name, kw...)
  end


  ####################
  # Helper Functions #
  ####################

  function schema_to_dict(schema)
    port_names = get_fields(schema)

    sym_to_q = Dict{Symbol, NamedQuery}()

    for name in keys(SQLOperators)
      new_q = Query()
      add_parts!(new_q, :Var, 2, var_type=nothing)
      add_part!(new_q, :Table, tab_name=name)
      add_parts!(new_q, :Column, 2, table=1, relation=[1,2],
                 col_name=SQLOperators[name][2], col_type=[nothing, nothing])
      add_part!(new_q, :Comparison, comp_port1=1, comp_port2=2)
      sym_to_q[name] = NamedQuery(new_q, SQLOperators[name][2])
    end

    for name in keys(port_names)
      fields = first.(port_names[name])
      types = Symbol.(getindex.(port_names[name], 2))

      new_q = Query()
      add_part!(new_q, :Table, tab_name=name)
      add_parts!(new_q, :Var, length(fields), var_type=nothing)
      add_parts!(new_q, :Column, length(fields), table=1,
                                                 relation=1:length(fields),
                                                 col_name=fields,
                                                 col_type=types)
      sym_to_q[name] = NamedQuery(new_q, fields)
    end
    sym_to_q
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

  function RelToQuery(rel, schema::Schema)
    q = oapply(rel, Dict(nq[1] => nq[2].query for nq in schema.relations))

    #set_subpart!(q.cospan.apex, :variable, subpart(rel, :variable))
    #add_parts!(q.cospan.apex, :OuterPort, nparts(rel, :OuterPort),
    #                                      outer_junction=subpart(rel, :outer_junction),
    #                                      outer_port_type=nothing)
    infer!(q.cospan.apex, [([:col_type],[:relation, :var_type]),
               ([:comp_port1,:col_type],[:comp_port2,:col_type])]);

    variables = subpart(rel, :variable)
    prep_var = findall(x -> string(x)[1] == '_', variables)
    add_parts!(q.cospan.apex, :Input, length(prep_var), input_var=prep_var)

    NamedQuery(q, variables[subpart(rel, :outer_junction)])
  end

  # Replication of CSet functionality
  # TODO: Fix this when ACSet Query functionality is added
  function subparts(acs::ACSet, names::Array{Symbol,1})
    collect(zip([subpart(acs, name) for name in names]...))
  end
end
