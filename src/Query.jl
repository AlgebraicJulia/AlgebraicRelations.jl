module QueryLib

export Query, make_query,
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  meet, top, FreeBicategoryRelations, @program, @relation,
  to_presentation, draw_query
  #, plus, zero, coplus, cozero,  join, bottom

using Catlab, Catlab.Theories, Catlab.Present,
      Catlab.WiringDiagrams, Catlab.Graphics,
      Catlab.Graphics.Graphviz
import Catlab.Theories:
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  plus, zero, coplus, cozero, meet, top, join, bottom, distribute_dagger,
  FreeBicategoryRelations
import Catlab.Programs: @program, @relation

using AutoHashEquals


""" Query

This structure holds the relationship graph between fields in a query

# Fields
- `types::Dict{Symbol, Tuple{Array{String,1}, Array{T,1} where T}}`:
          The mapping between a type symbols and their fundamental types along
          with the field names for those types if the SQL type is composite.
- `tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}`:
          The mapping between a table symbols and their column names for domain
          and codomain.
- `wd::WiringDiagram`: The wiring diagram which holds the relational
                       information for the query.
"""
struct Query{WD}
  types::Dict{Symbol, Tuple{Array{String,1}, Array{T,1} where T}}
  tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}
  wd::WD
  Query(types, tables, wd::WiringDiagram) = new{WiringDiagram}(types, tables, merge_junctions(wd))
  Query(types, tables, wd::UndirectedWiringDiagram) = new{UndirectedWiringDiagram}(types, tables, wd)
end

function Query(wd::WiringDiagram)::Query
  n_types = Dict{Symbol, Tuple{Array{String,1}, Array{<:Type,1}}}()
  n_table = Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}()
  Query(n_types, n_table, wd)
end

# Define a query based off of a formula and a table of column names
function Query(types, 
               tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}},
               q::GATExpr)
  Query(types, tables, to_wiring_diagram(q))
end

# Generate a Catlab Presentation from homs and obs
to_presentation(types::Array{<:GATExpr{:generator},1},
                tables::Array{<:GATExpr{:generator},1})::Presentation = begin
  p = Presentation(FreeBicategoryRelations)
  add_generators!(p, types)
  add_generators!(p, tables)
  return p
end

# Draw a query wiring diagram
draw_query(q::Query{WiringDiagram})::Graph = begin
  to_graphviz(q.wd, orientation=LeftToRight, labels=true)
end

# Draw an undirected query wiring diagram
draw_query(q::Query{UndirectedWiringDiagram}; edge_attrs=Dict(:len => "1.0"))::Graph = begin
  to_graphviz(q.wd, box_labels=:name, edge_attrs=edge_attrs)
end
end
