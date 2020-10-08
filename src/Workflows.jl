module Workflows

  using ..DB

  using Catlab
  using Catlab.Theories
  using Catlab.Theories.FreeSchema: Attr, Data
  import Catlab.Theories: FreeSymmetricMonoidalCategory

  export wf_to_schema, @workflow, FreeSymmetricMonoidalCategory

  macro workflow(exprs...)
    Expr(:call, GlobalRef(Queries, :parse_query_statement),
                esc(schema), exprs)
  end

  function wf_to_schema(wf::Presentation, field_names::Dict{Symbol, Array{Symbol,1}}, type_map::Dict{Symbol, Symbol})
    gens = Array{GATExpr, 1}()
    tables = Dict{Symbol, GATExpr}()
    sym_app(s::Symbol, suffix::String) = Symbol(string(s, suffix))
    get_syms(g::Union{GATExpr, Array, Symbol}) = begin
      if g isa Array
        sym_array = Array{Symbol, 1}()
        for i in g
          append!(sym_array, get_syms(i))
        end
        return sym_array
      elseif hasproperty(g, :args)
        return get_syms(g.args)
      elseif g isa Symbol
        return [g]
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
    #  push!(gens, Attr(sym_app(tab_name, "_1_data"), table, generator(TheorySQL, type_map[g_name])))
    #end

    # Evaluate homs to purely data-connected tables
    for g in generators(wf, :Hom)
      g_name = g.args[1]
      table = Ob(FreeSchema, g_name)
      tables[g_name] = table
      push!(gens, table)
      append!(gens, map(enumerate(get_syms(g.type_args))) do (i, sym)
                Attr(sym_app(g_name, "_$(i)_$(field_names[g_name][i])"), table, generator(TheorySQL, type_map[sym]))
            end
            )
    end

    @present p <: TheorySQL begin end

    add_generators!(p, gens)
    p
  end

end
