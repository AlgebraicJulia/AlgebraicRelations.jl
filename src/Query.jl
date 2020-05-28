module QueryLib

export Ports, Query, make_query,
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  meet, top, to_sql, FreeBicategoryRelations
  #, plus, zero, coplus, cozero,  join, bottom

using Catlab, Catlab.Doctrines, Catlab.Present, Catlab.WiringDiagrams
import Catlab.Doctrines:
  Ob, Hom, dom, codom, compose, ⋅, ∘, id, otimes, ⊗, munit, braid, σ,
  dagger, dunit, dcounit, mcopy, Δ, delete, ◊, mmerge, ∇, create, □,
  plus, zero, coplus, cozero, meet, top, join, bottom, distribute_dagger,
  FreeBicategoryRelations

using AutoHashEquals
import Schema.Presentation: Schema, TypeToSql

@auto_hash_equals struct Types
  ports::Ports
end

""" Query

This structure holds the relationship graph between fields in a query

# Fields
- `dom::Types`: the types of the columns in the domain
- `codom::Types`: the types of the columns in the codomain
- `dom_names::Array{Int,1}`: index of the domain fields
- `codom_names::Array{Int,1}`: index of the codomain fields
- `tables::Array{String,1}`: Names of the tables included in the relationship
                             graph
- `fields::Array{Tuple{Int,String},1}`: Connection between a table and its
                                        fields
- `edges::Array{Tuple{Int,Int},1}`: Equality relationship between fields of
                                    tables
"""
struct Query
  tables::Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}
  wd::WiringDiagram
end

Query(wd::WiringDiagram)::Query = begin
  n_table = Dict{Symbol, Tuple{Array{String,1}, Array{String,1}}}()
  Query(n_table, wd)
end

struct Vertex
  name::Symbol
  is_dagger::Bool
  is_junc::Bool
end

function to_sql(q::Query)::String

  ind_to_alias(i) = "t$i"
  wd = q.wd
  tables = q.tables

  # Get the vertices as (name, is_dagger, is_junc)
  verts = fill(Vertex(:*,false,false),nboxes(wd)+2)
  verts[1] = Vertex(:input,false,true)
  verts[2] = Vertex(:output,false,true)

  for k in box_ids(wd)
    v = box(wd, k)

    is_junc   = false
    is_dagger = false
    name      = :*
    if typeof(v) <: Junction
      is_junc = true
    elseif typeof(v) <: BoxOp{:dagger}
      is_dagger = true
      name = v.box.value
    else
      name = v.value
    end
    verts[k] = Vertex(name, is_dagger, is_junc)
  end

  # Make the join statement
  alias_array = Array{String,1}()
  for v in box_ids(wd)
    cur_b = verts[v]
    name = string(cur_b.name)
    if !cur_b.is_junc
      push!(alias_array, "$name AS $(ind_to_alias(v))")
    end
  end

  # Make the relation statement
  relation_array = Array{String,1}()

  # Neighbors will keep track of connections to junction nodes
  neighbors = fill((Array{String,1}(),Array{String,1}()), length(verts))

  neighbors[1] = (Array{String,1}(),fill("", length(wd.input_ports)))
  neighbors[2] = (fill("", length(wd.output_ports)),Array{String,1}())
  for i in 3:length(neighbors)
    if verts[i].is_junc
      neighbors[i] = ([""],[""])
    else
      neighbors[i] = (fill("",length(tables[verts[i].name][1])),
                      fill("",length(tables[verts[i].name][2])))
    end
  end

  for e in wires(wd)
    sb = e.source.box
    sp = e.source.port
    db = e.target.box
    dp = e.target.port
    src = ""
    dst = ""

    if verts[sb].is_junc && verts[db].is_junc
      continue
    elseif verts[sb].is_junc
      df = tables[verts[db].name][1][dp]
      if verts[db].is_dagger
        df = tables[verts[db].name][2][dp]
      end
      if neighbors[sb][2][sp] == ""
        neighbors[sb][2][sp] = "$(ind_to_alias(db)).$df"
        continue
      else
        src = neighbors[sb][2][sp]
        dst = "$(ind_to_alias(db)).$df"
      end
    elseif verts[db].is_junc
      sf = tables[verts[sb].name][2][sp]
      if verts[sb].is_dagger
        sf = tables[verts[sb].name][1][sp]
      end
      if neighbors[db][1][dp] == ""
        neighbors[db][1][dp] = "$(ind_to_alias(sb)).$sf"
        continue
      else
        src = neighbors[db][1][dp]
        dst = "$(ind_to_alias(sb)).$sf"
      end
    else
      sf = tables[verts[sb].name][2][sp]
      if verts[sb].is_dagger
        sf = tables[verts[sb].name][1][sp]
      end
      src = "$(ind_to_alias(sb)).$sf"

      df = tables[verts[db].name][1][dp]
      if verts[db].is_dagger
        df = tables[verts[db].name][2][dp]
      end
      dst = "$(ind_to_alias(db)).$df"
    end

    if dst*"="*src in relation_array || src*"="*dst in relation_array
      continue
    end
    push!(relation_array, src*"="*dst)
  end

  # The only important junction nodes are the input/output nodes
  dom_array = neighbors[1][2]
  codom_array = neighbors[2][1]

  select = "SELECT "*join(vcat(dom_array, codom_array), ", ")*"\n"
  from = "FROM "*join(alias_array, ", ")*"\n"
  condition = "WHERE "*join(relation_array, " AND ")*";"

  return select*from*condition
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

make_query(s::Schema, q::GATExpr)::Query = begin

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
end
