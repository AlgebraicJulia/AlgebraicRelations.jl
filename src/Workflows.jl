module Workflows

  using ..DB

  using Catlab
  using Catlab.Theories
  using Catlab.Graphics
  using Catlab.Theories.FreeSchema: Attr, Data
  import Catlab.Theories: FreeSymmetricMonoidalCategory, ⊗
  import Catlab.Programs: @program

  export wf_to_schema, @program, draw_workflow, FreeSymmetricMonoidalCategory,
         add_product!, add_products!, add_process!, add_processes!, Workflow, ⊗

  function Workflow()
    return Presentation(FreeSymmetricMonoidalCategory)
  end

  function add_product!(p::Presentation, field::Tuple{Symbol, Symbol})
    ob = Ob(FreeSymmetricMonoidalCategory, field[1])
    push!(ob.args, field[2])
    add_generator!(p, ob)
    return ob
  end

  function add_products!(p::Presentation, fields::Array{Tuple{Symbol, Symbol}})
    return map(field->add_product!(p, field), fields)
  end

  function add_process!(p::Presentation, hom::Tuple{Symbol, GATExpr, GATExpr})
    h = Hom(hom...)
    add_generator!(p, h)
    return h
  end

  function add_processes!(p::Presentation, homs::Array{<:Tuple{Symbol, <:GATExpr, <:GATExpr},1})
    return map(hom->add_process!(p, hom), homs)
  end

  function wf_to_schema(wf::Presentation)
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
    p
  end

  function draw_workflow(p)
    to_graphviz(p)
  end
end
