# ACSet Interface

get_table(column) = From(:Table=>:tname)|>
                    Where(:Table, From(:Column=>:table)|>Where(:cname, column))

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
    subpart(fabric, :, column) 
end
export subpart

# TODO
# function ACSetInterface.subpart(fabric::DataFabric, column::PK{T<:ACSet})
#     nparts(fabric.graph
# end

function ACSetInterface.subpart(fabric::DataFabric, fks::Vector{FK{T}}, column::Symbol) where T
    subpart(fabric, getproperty.(fks, :val), column)
end

function ACSetInterface.subpart(fabric::DataFabric, id, column::Symbol)
    column = if isempty(incident(fabric.catalog, column, :tname))
        column
    else
        Symbol("$(column)_id")
    end
    source = decide_source(fabric, :cname => column)
    tableid = subpart(fabric.catalog, incident(fabric.catalog, column, :cname), :table)
    table = subpart(fabric.catalog, tableid, :tname) |> only
    # TODO move handling of FK types to another method
    id = eltype(id) <: FK ? getproperty.(id, :val) : id
    subpart(source, id, table => column)
end

# Winemaker => name
function ACSetInterface.subpart(fabric::DataFabric, id, column::Pair{Symbol, Symbol})
    # get columns
    columns = subpart(fabric.catalog, incident(fabric.catalog, column.first, [:table, :tname]), :cname)
    @assert !isempty(columns[columns .== column.second])
    # TODO simplify
    source = subpart(fabric.graph, 
                     subpart(fabric.catalog,
                             # get the id of the table
                             incident(fabric.catalog, column.first, :tname),
                             :source), 
                     :value)
    subpart(only(source), id, column.second)
end

function ACSetInterface.subpart(fabric::DataFabric, column::Pair{Symbol, Symbol})
    subpart(fabric, :, column)
end

function ACSetInterface.subpart(fabric::DataFabric, columns::Vector{Symbol})
    subpart(fabric, :, columns)
end

# TODO wrap in datA frame here
function ACSetInterface.subpart(fabric::DataFabric, id=(:), columns::Vector{Symbol}=[]; formatter=identity)
    out = reduce((new_id, column) -> subpart(fabric, new_id, column), columns; init=id)
    formatter(out)
end

function ACSetInterface.incident(fabric::DataFabric, id, column::Symbol; formatter=identity)
    # TODO could be multiple
    source = decide_source(fabric, :cname => column)
    _, table = get_table(column)(fabric.catalog) |> only
    out = incident(source, id, only(table) => column)
    formatter(out)
end
export incident

# TODO does not work with DataStore
function ACSetInterface.incident(fabric::DataFabric, id, columns::Vector{Symbol}; formatter=identity)
    out = reduce((new_id, column) -> incident(fabric, new_id, column), columns; init=id)
    formatter(out)
end

function ACSetInterface.incident(fabric::DataFabric, value, tablecol::Tuple{Symbol, Symbol})
    source = decide_source(fabric, :tname => tablecol)
    incident(source, value, tablecol[2])
end

