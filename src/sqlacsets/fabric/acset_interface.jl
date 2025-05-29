# ACSet Interface

get_table(column) = From(:Table=>:tname)|>
                    Where(:Table, From(:Column=>:table)|>Where(:cname, column))

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

function ACSetInterface.subpart(fabric::DataFabric, fks::Vector{FK{T}}, column::Symbol; formatter=identity) where T
    out = subpart(fabric, getproperty.(fks, :val), column)
    formatter(out)
end

function ACSetInterface.subpart(fabric::DataFabric, id, column::Symbol; formatter=identity)
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
    if !isempty(id)
        out = subpart(source, id, table => column)
        formatter(out)
    else
        []
    end
end

# Winemaker => name
function ACSetInterface.subpart(fabric::DataFabric, id, column::Pair{Symbol, Symbol}; formatter=identity)
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
    out = subpart(only(source), id, column.second)
    formatter(out)
end

function ACSetInterface.subpart(fabric::DataFabric, column::Pair{Symbol, Symbol}; formatter=identity)
    out = subpart(fabric, :, column)
    formatter(out)
end

function ACSetInterface.subpart(fabric::DataFabric, columns::Vector{Symbol}; formatter=identity)
    out = subpart(fabric, :, columns)
    formatter(out)
end

# TODO wrap in datA frame here
function ACSetInterface.subpart(fabric::DataFabric, id=(:), columns::Vector{Symbol}=[]; formatter=identity)
    out = reduce((new_id, column) -> subpart(fabric, new_id, column), columns; init=id)
    formatter(out)
end

function another(X::ACSet, val, col::Symbol, other::Symbol)
    subpart(X, incident(X, val, col), other)
end

function ACSetInterface.incident(fabric::DataFabric, id, column::Symbol; formatter=identity)
    if column ∈ [:id, :_id]
        return formatter(id)
    end
    # TODO could be multiple
    source = decide_source(fabric, :cname => column)
    _, table = get_table(column)(fabric.catalog) |> only
    # TODO I don't like calling other here. 
    # what if the column (`val`) had the same name as another, so `incident` in `another` returns |vector|>1?
    fkmaybe = let T = only(another(fabric.catalog, column, :cname, :type))
        T <: FK ? T : identity
    end
    # TODO we broadcast over ids, which excludes vector-valued data
    out = incident(source, fkmaybe.(id), only(table) => column)
    formatter(out)
end
export incident

# incident(fabric, (3, :b), ([3,4], :c))
function ACSetInterface.incident(fabric::DataFabric, kvs::Vector{Tuple{<:T, Symbol}}; formatter=identity) where T
    ids = map([incident(fabric, val, col; formatter=identity) for (val,col) in kvs]) do result
        result isa DataFrame ? result._id : result
    end
    isempty(ids) && return []
    out = intersect(ids...)
    # TODO if a result is a DataFrame, then we have have an issue
    formatter(out)
end
# TODO returns ids of matching rows

# TODO does not work with DataStore
function ACSetInterface.incident(fabric::DataFabric, id, columns::Vector{Symbol}; formatter=identity)
    @info columns
    out = reduce((new_id, column) -> incident(fabric, new_id, column), columns; init=id)
    formatter(out)
end

# calls ultimately go into this
function ACSetInterface.incident(fabric::DataFabric, value, tablecol::Tuple{Symbol, Symbol}; formatter=identity)
    source = decide_source(fabric, :tname => tablecol)
    @warn tablecol
    out = incident(source, value, tablecol[2])
    formatter(out)
end

