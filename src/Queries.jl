module Queries

export to_funsql, SQLTable

using Catlab
using Catlab.CategoricalAlgebra
using Catlab.RelationalPrograms: UntypedNamedRelationDiagram
using JSON
using FunSQL
using FunSQL: Where, Join, Select, Get, render, From, As, Fun, SQLNode
import FunSQL: SQLTable

using ..Schemas

function SQLTable(sch::SQLSchema)
    Dict{Symbol, Union{SQLTable, SQLNode}}(map(parts(sch, :Table)) do t
        tname = Symbol(lowercase(sch[t, :tname]))
        tname => SQLTable(tname, columns = Symbol.(sch[incident(sch, t, :table), :cname]))
    end)
end

# convert Rel to FunSQL
function to_funsql(rel, sch::SQLSchema; queries::Dict{Symbol, SQLNode} = Dict{Symbol, SQLNode}())
    to_funsql(rel, merge(SQLTable(sch), queries))
end

lc(v::Vector{Symbol}) = Symbol.(lowercase.(string.(v)))

function to_funsql(rel::UntypedNamedRelationDiagram, schema::Dict{Symbol, Union{SQLTable, SQLNode}})
    included_tables = fill(false, nparts(rel, :Box))
    box_uid = [Symbol("b$i") for i in 1:nparts(rel, :Box)]
    j_value = Vector{Any}([nothing for _ in 1:nparts(rel, :Junction)])
    to_include = [1]
    set_subpart!(rel, :name, lc(rel[:name])) # use map?
    funsql = schema[rel[1, :name]] isa SQLNode ?
                (schema[rel[1, :name]] |> As(box_uid[1])) :
                (From(schema[rel[1, :name]]) |> As(box_uid[1]))
    # algorithm
    while(!isempty(to_include))
      cur_box = pop!(to_include)
      included_tables[cur_box] = true
      ports = incident(rel, cur_box, :box)
      join_rels = Vector{Any}() 
      for p in ports
        jctn = rel[p, :junction]
        get_p = Get[box_uid[cur_box]][rel[p,:port_name]]
        if isnothing(j_value[jctn])
          j_value[jctn] = get_p
          for ip in incident(rel, jctn, :junction)
            ib = rel[ip, :box]
            if !(ib ∈ to_include || included_tables[ib])
              push!(to_include, ib)
            end
          end
        else
          push!(join_rels, j_value[jctn] .== get_p)
        end
      end
      if cur_box != 1
          funsql = Join(box_uid[cur_box] => schema[rel[cur_box, :name]];
                      on = isempty(join_rels) || Fun.and(join_rels...))(funsql)
      end
    end
    funsql |> Select(map(j -> rel[j, :outer_port_name]=>j_value[rel[j, :outer_junction]], 1:nparts(rel, :OuterPort))...)
end

# convert FunSQL to Rel
function relation(catalog::FunSQL.SQLCatalog, n::FunSQL.SQLNode) end

end
