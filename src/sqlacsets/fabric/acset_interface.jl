# ACSet Interface

# TODO refactor to use graph
function decide_source(fabric::DataFabric, attr::Pair{Symbol, Tuple{Symbol, Symbol}})
    id = incident(fabric.catalog, attr.second[1], attr.first)
    source_id = subpart(fabric.catalog, only(id), :source)
    subpart(fabric.graph, source_id, :value)
end

function decide_source(fabric::DataFabric, attr::Pair{Symbol, Symbol})
    id = incident(fabric.catalog, attr.second, attr.first)
    if attr.first == :cname
        id = subpart(fabric.catalog, id, :table)
    end
    @assert length(id) == 1
    source_id = subpart(fabric.catalog, id, :source)
    source = subpart(fabric.graph, source_id, :value)
    only(source)
end

function ACSetInterface.add_part!(fabric::DataFabric, table::Symbol, args...)
    source = decide_source(fabric, :tname => table)
    add_part!(source, table, args...)
end
export add_part!

function ACSetInterface.nparts(fabric::DataFabric, table::Symbol)
    source = decide_source(fabric, :tname => table)
    nparts(source, table) 
end
export nparts

function ACSetInterface.maxpart(fabric::DataFabric, table::Symbol)
    source = decide_source(fabric, :tname => table)
    maxpart(source, table)
end
export maxpart

function ACSetInterface.subpart(fabric::DataFabric, column::Symbol)
    column = if isempty(incident(fabric.catalog, column, :tname))
        column
    else
        Symbol("$(column)_id")
    end
    source = decide_source(fabric, :cname => column)
    tableid = subpart(fabric.catalog, incident(fabric.catalog, column, :cname), :table)
    table = subpart(fabric.catalog, tableid, :tname) |> only
    subpart(source, :, table => column)
end
export subpart

function ACSetInterface.subpart(fabric::DataFabric, id, column::Pair{Symbol, Symbol})
    # get columns
    columns = subpart(fabric.catalog, incident(fabric.catalog, column.first, [:table, :tname]), :cname)
    @assert !isempty(columns[columns .== column.second])
    source = subpart(fabric.catalog, 
                     subpart(fabric.catalog, 
                             incident(fabric.catalog, column.first, :tname),
                             :source), 
                     :conn)
    subpart(only(source), id, column.second)
end

function ACSetInterface.incident(fabric::DataFabric, id, column)
    source = decide_source(fabric, :cname => column)
    table = subpart(fabric.catalog, subpart(fabric.catalog, incident(fabric.catalog, column, :cname), :table), :tname) |> only
    table = Symbol(lowercase(string(table)))
    incident(source, id, table => column)
end
export incident

function ACSetInterface.incident(fabric::DataFabric, value, tablecol::Tuple{Symbol, Symbol})
    source = decide_source(fabric, :tname => tablecol)
    incident(source, value, tablecol[2])
end

