module QueryLib

export Ports, Query, make_query,
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  meet, top, FreeBicategoryRelations, to_presentation, draw_query
  #, plus, zero, coplus, cozero,  join, bottom

using Catlab, Catlab.Doctrines, Catlab.Present,
      Catlab.WiringDiagrams, Catlab.Graphics,
      Catlab.Graphics.Graphviz
import Catlab.Doctrines:
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  plus, zero, coplus, cozero, meet, top, join, bottom, distribute_dagger,
  FreeBicategoryRelations

using AutoHashEquals

import AlgebraicRelations.SchemaLib: Schema

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
  types::Dict{Symbol, Tuple{Array{String,1}, Array{T,1} where T}}
  tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}
  wd::WiringDiagram
  Query(types, tables, wd) = new(types, tables, merge_junctions(wd))
end

Query(wd::WiringDiagram)::Query = begin
  n_types = Dict{Symbol, Tuple{Array{String,1}, Array{<:Type,1}}}()
  n_table = Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}()
  Query(n_types, n_table, wd)
end

@instance BicategoryRelations(Types, Query) begin

  dom(f::Query)   = input_ports(Types, f.wd)
  codom(f::Query) = output_ports(Types, f.wd)
  munit(::Type{Types}) = Types(Ports([]))

  compose(f::Query, g::Query) = begin
    n_types = merge(f.types, g.types)
    n_tables = merge(f.tables, g.tables)
    n_wd = compose(f.wd, g.wd)
    return Query(n_types, n_tables, n_wd)
  end

  otimes(A::Types, B::Types) = Types(otimes(A.ports,B.ports))
  otimes(f::Query, g::Query) = begin
    n_types = merge(f.types, g.types)
    n_tables = merge(f.tables, g.tables)
    n_wd = otimes(f.wd, g.wd)
    return Query(n_tables, n_wd)
  end

  meet(f::Query, g::Query) = begin
    n_types = merge(f.types, g.types)
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

# Define a query based off of a formula and a table of column names
Query(types, tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}},
      q::GATExpr) = begin
  Query(types, tables, to_wiring_diagram(q))
end

# Generate a Catlab Presentation from homs and obs
to_presentation(types::Array{<:GATExpr{:generator},1},
                tables::Array{<:GATExpr{:generator},1})::Presentation = begin
  p = Presentation()
  add_generators!(p, types)
  add_generators!(p, tables)
  return p
end

# Draw a query wiring diagram
draw_query(q::Query)::Graph = begin
  to_graphviz(q.wd, orientation=LeftToRight, labels=true)
end
end
