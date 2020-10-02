module ACSetQueries
  using Catlab: @present
  import Catlab.Programs.RelationalPrograms: TheoryTypedRelationDiagram
  import Catlab.Programs.RelationalPrograms: parse_relation_diagram
  using Catlab.CategoricalAlgebra.CSets
  using ..ACSetDB

  # Used for the redefinition of copy_parts!
	using Catlab.Theories: Schema, FreeSchema, dom, codom,
	  CatDesc, CatDescType, AttrDesc, AttrDescType, SchemaType,
	  ob_num, hom_num, data_num, attr_num, dom_num, codom_num
	
  export TheoryQuery, Query, @query

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
	    port_name = port_names[box_name][port_per_box[i]][1]
	  end
	  set_subparts!(q, 1:nparts(q, :Port), field=port_names)
	  q
	end

	macro query(schema, exprs...)
    Expr(:call, GlobalRef(ACSetQueries, :parse_query_statement),
                esc(schema), exprs)
#    return :(parse_query_statement(esc($schema), $exprs))
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
