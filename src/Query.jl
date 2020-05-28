module QueryLib

export Ports, Query, make_query,
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  meet, top, FreeBicategoryRelations
  #, plus, zero, coplus, cozero,  join, bottom

using Catlab, Catlab.Doctrines, Catlab.Present, Catlab.WiringDiagrams
import Catlab.Doctrines:
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  plus, zero, coplus, cozero, meet, top, join, bottom, distribute_dagger,
  FreeBicategoryRelations

using AutoHashEquals

import AlgebraicRelations.Presentation: Schema

@auto_hash_equals struct Types
  ports::Ports
end

""" Query

This structure holds the relationship graph between fields in a query

# Fields
- `tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}`:
          The mapping between a table symbols and their column names for domain
          and codomain.
- `wd::WiringDiagram`: The wiring diagram which holds the relational 
                       information for the query.
"""
struct Query
  tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}
  wd::WiringDiagram
end

Query(wd::WiringDiagram)::Query = begin
  n_table = Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}()
  Query(n_table, wd)
end

@instance BicategoryRelations(Types, Query) begin

  dom(f::Query)   = input_ports(Types, f.wd)
  codom(f::Query) = output_ports(Types, f.wd)
  munit(::Type{Types}) = Types(Ports([]))

  compose(f::Query, g::Query) = begin
    n_tables = merge(f.tables, g.tables)
    n_wd = compose(f.wd, g.wd)
    return Query(n_tables, n_wd)
  end

  otimes(A::Types, B::Types) = Types(otimes(A.ports,B.ports))
  otimes(f::Query, g::Query) = begin
    n_tables = merge(f.tables, g.tables)
    n_wd = otimes(f.wd, g.wd)
    return Query(n_tables, n_wd)
  end

  meet(f::Query, g::Query) = begin
    n_tables = merge(f.tables, g.tables)
    n_wd = meet(f.wd, g.wd)
    return Query(n_tables, n_wd)
  end

  dagger(f::Query) = Query(f.tables, dagger(f.wd))

  dunit(A::Types) = begin
    Query(dunit(A.ports))
  end

  top(A::Types, B::Types) = begin
    Query(top(A.ports,B.ports))
  end

  dcounit(A::Types) = begin
    Query(dcounit(A.ports))
  end

  id(A::Types) = begin
    Query(id(A.ports))
  end

  braid(A::Types, B::Types) = begin
    Query(braid(A.ports,B.ports))
  end

  mcopy(A::Types) = begin
    Query(implicit_mcopy(A.ports,2))
  end

  mmerge(A::Types) = begin
    Query(implicit_mmerge(A.ports,2))
  end

  delete(A::Types) = begin
    Query(delete(A.ports))
  end

  create(A::Types) = begin
    Query(create(A.ports))
  end
end

Query(s::Schema, q::GATExpr)::Query = begin

  # Keep the type names associated to consistent Types objects
  new_type = Dict{FreeBicategoryRelations.Ob,FreeBicategoryRelations.Ob}()

  # Generate Types objects from object description
  q_types = map(s.types) do t

    # Symbol name is used for type
    sym = t.args[1][1]
    A = Ob(FreeBicategoryRelations, sym)
    new_type[t] = A
    return Types(to_wiring_diagram(new_type[t]))
  end

  # Generate the Query objects from hom descriptions
  q_homs = map(s.relations) do t

    dom_type    = Array{Types, 1}()
    codom_type  = Array{Types, 1}()
    dom_name    = Array{String,1}()
    codom_name  = Array{String,1}()

    # First do domain, then codomain
    names = t.args[1].fields[1]
    types = t.args[2]

    # Check if domain is composition
    if typeof(types) <: Catlab.Doctrines.FreeBicategoryRelations.Ob{:otimes}
      dom_name = names
      dom_type = map(types.args) do cur_t
        return new_type[cur_t]
      end
    else
      dom_name = [names]
      dom_type = [new_type[types]]
    end

    names = t.args[1].fields[2]
    types = t.args[3]
    # Check if codomain is composition
    if typeof(types) <: Catlab.Doctrines.FreeBicategoryRelations.Ob{:otimes}
      codom_name = names
      codom_type = map(types.args) do cur_t
        return new_type[cur_t]
      end
    else
      codom_name = [names]
      codom_type = [new_type[types]]
    end


    tables = Dict(t.args[1].name => (dom_name, codom_name))

    hom = Hom(t.args[1].name, otimes(dom_type), otimes(codom_type))
    Query(tables, to_wiring_diagram(hom))
  end

  d = Dict()
  for i in 1:length(q_types)
    d[s.types[i]] = q_types[i]
  end
  for i in 1:length(q_homs)
    d[s.relations[i]] = q_homs[i]
  end
  functor((Types, Query), q, generators=d)
end

# Define a query based off of a formula and a table of column names
Query(tables::Dict{Symbol, Tuple{Array{String,1},Array{String,1}}}, 
      q::GATExpr) = begin
  Query(tables, to_wiring_diagram(q))
end

end
