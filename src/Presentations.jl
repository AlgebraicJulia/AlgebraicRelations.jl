module Presentations

  using ..DB

  using Catlab
  using Catlab.Theories
  using Catlab.Graphics
  using Catlab.Present: Presentation
  using Catlab.Theories.FreeSchema: Attr, Data
  using AlgebraicPetri
  using Catlab.WiringDiagrams.DirectedWiringDiagrams
  import Catlab.Theories: FreeSymmetricMonoidalCategory, ⊗
  import Catlab.Programs: @program
  using Catlab.CategoricalAlgebra
  using Dagger

  export present_to_schema, @program, draw_workflow, FreeSymmetricMonoidalCategory,
         add_types!, add_type!, add_process!, add_processes!, Presentation, ⊗,
         draw_schema, evaluate

  Presentation() = Presentation(FreeSymmetricMonoidalCategory)

  function add_type!(p::Presentation, field::Tuple{Symbol, <:Type})
    ob = Ob(FreeSymmetricMonoidalCategory, field[1])
    push!(ob.args, Symbol(field[2]))
    add_generator!(p, ob)
    return ob
  end

  function add_types!(p::Presentation, fields::Array{<:Tuple{Symbol, Type}})
    return map(field->add_type!(p, field), fields)
  end

  function add_process!(p::Presentation, hom::Tuple{Symbol, GATExpr, GATExpr})
    h = Hom(hom...)
    add_generator!(p, h)
    return h
  end

  function add_processes!(p::Presentation, homs::Array{<:Tuple{Symbol, <:GATExpr, <:GATExpr},1})
    return map(hom->add_process!(p, hom), homs)
  end

  function present_to_schema(wf::Presentation)
    gens = Array{GATExpr, 1}()
    tables = Dict{Symbol, GATExpr}()
    sym_app(s::Symbol, suffix::String) = Symbol(string(s, suffix))
    get_syms(g::Union{GATExpr, Array}) = begin
      if g isa Array
        if eltype(g) == Symbol
          return [g]
        end
        sym_array = Array{Array{Symbol, 1}, 1}()
        for i in g
          append!(sym_array, get_syms(i))
        end
        return sym_array
      elseif hasproperty(g, :args)
        return get_syms(g.args)
      end
    end

    # Evaluate objects to tables with attributes
    #for g in generators(wf, :Ob)
    #  g_name = g.args[1]
    #  tab_name = sym_app(g_name, "_T")
    #  table = Ob(FreeSchema, tab_name)
    #  tables[g_name] = table
    #  push!(gens, table)
    #  push!(gens, Attr(sym_app(tab_name, "_1_id"), table, generator(TheorySQL, :Int64)))
    #  push!(gens, Attr(sym_app(tab_name, "_1_data"), table, generator(TheorySQL, g.args[2])))
    #end

    # Evaluate homs to purely data-connected tables
    for g in generators(wf, :Hom)
      g_name = g.args[1]
      table = Ob(FreeSchema, g_name)
      tables[g_name] = table
      push!(gens, table)
      append!(gens, map(enumerate(get_syms(g.type_args))) do (i, sym)
                Attr(sym_app(g_name, "_$(i)_$(sym[1])$i"),
                     table, generator(TheorySQL, sym[2]))
      end)
    end

    @present p <: TheorySQL begin end
    add_generators!(p, gens)
    SchemaType(p)
  end

  function draw_schema(p::Presentation; kw...)
    ob_names = Symbol.(generators(p, :Ob))
    hom_names = Symbol.(generators(p, :Hom))
    hom_dict = Dict{Symbol, Tuple}()
    for hom in generators(p, :Hom)
      # Evaluate Dom
      dom_v = if eltype(dom(hom).args) <: GATExpr
        Tuple(Symbol.(dom(hom).args))
      else
        Symbol(dom(hom))
      end
      # Operate on Codom
      codom_v = if eltype(codom(hom).args) <: GATExpr
        Tuple(Symbol.(codom(hom).args))
      else
        Symbol(codom(hom))
      end

      hom_dict[Symbol(hom)] = (dom_v, codom_v)
    end
    schema_ap = LabelledPetriNet(ob_names, hom_dict...)
    Graph(schema_ap)
  end

  function draw_workflow(p; kw...)
    to_graphviz(p; orientation=LeftToRight, kw...)
  end

  function evaluate(dwd::WiringDiagram, funcs::Dict{Symbol, <:Function},
                     input_vals::Array)
    graph = dwd.graph
    g_inputs = [wire.source.port for wire in subpart(graph, incident(graph, 1, :src),
                                                     :wire)]
    g_outputs = incident(graph, 2, :tgt)
    g_out_ports = [wire.target.port for wire in subpart(graph,g_outputs,:wire)]
    g_out_ports[g_out_ports] .= g_outputs

    values = Array{Thunk, 1}(undef, nparts(graph, :E))
    values[incident(graph, 1, :src)] .= [delayed(x->x)(input_vals[i]) for i in g_inputs]

    evaluated = fill(false, nparts(graph, :V))
    available = fill(false, nparts(graph, :E))
    evaluated[1:2] .= true
    available[incident(graph, 1, :src)] .= true

    while !all(evaluated)
      mod = false;
      for i in 3:nparts(graph, :V)
        inputs = incident(graph, i, :tgt)
        outputs = incident(graph, i, :src)
        out_ports = [wire.source.port for wire in subpart(graph,
                                                          outputs,
                                                          :wire)]
        in_ports = [wire.target.port for wire in subpart(graph,inputs,:wire)]
        in_ports[in_ports] .= inputs
        if !evaluated[i] && all(available[inputs])
          func = delayed(funcs[subpart(graph, i, :box).value])(values[in_ports]...)
          values[outputs] .= [delayed(x->x[i])(func) for i in out_ports]
          evaluated[i] = true
          available[outputs] .= true
          mod = true;
        end
      end
      if !mod
          error("Not all boxes are able to be evaluated")
      end
    end
    return collect(delayed((x...)->x)(values[g_out_ports]...))
  end

end
