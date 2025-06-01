"""
Prints a helpful message that the current `DataFabric` is empty to stdout.

# Returns

- Single vector of a string that says "DataFabric Is Empty".
"""
function _empty_fabric_msg()
    print(Base.stdout, "DataFabric currently empty. Please add a source by doing:\n\n")

    printstyled(Base.stdout, "using Catlab\n", color = :light_blue, bold = true)
    printstyled(Base.stdout, "using ACSets\n", color = :light_blue, bold = true)
    printstyled(Base.stdout, "using AlgebraicRelations\n", color = :light_blue, bold = true)
    printstyled(Base.stdout, "\nfabric = DataFabric();\n", color = :light_blue, bold = true)
    printstyled(Base.stdout, "add_source!(fabric, source)\n", color = :light_blue, bold = true)

    println(Base.stdout, "\nin your REPL or code.\n")

    return ["Data fabric is empty"] 
end

"""
```julia
_source_summary(fabric::DataFabric)
```

Internal function to generate summary of a `DataFabric` object.

# Arguments

- `fabric::DataFabric` - `DataFabric` object

# Returns

- `source_summary::Matrix` - a matrix representing the summarized information of a `DataFabric`. Each column corresponds to the following information:

    - `Identifier` - internal ID assigned to source

    - `Name` - plaintext name of a source

    - `PrimaryKey` - primary key of a source

    - `Type` - type of a source

    - `Fields` - how many fields a source has

    - `Indegree` - the indegree of a source

    - `Outdegree` - the outdegree of a source

- `source_summary_cols::Vector` - the names of each column in the `source_summary`.

# Example

```julia-repl
julia> summary, colnames = _source_summary(fabric)
```

"""
function _source_summary(fabric::DataFabric) 
    subparts = fabric.catalog.subparts
    source_maps = subparts.tname.m
    source_froms = [subparts.table.m[v] for v in values(subparts.from.m) .|> Int]
    source_tos = [subparts.table.m[v] for v in values(subparts.to.m) .|> Int]

    source_keys = keys(source_maps) .|> Int
    source_values = values(source_maps) .|> Symbol
    source_types = [subparts.conn.m[k] for k in source_keys]

    source_froms_count = [(i, count(==(i), source_froms)) for i in source_keys] |> Dict
    source_tos_count = [(i, count(==(i), source_tos)) for i in source_keys] |> Dict
    source_fields_count = [(i, count(==(i), values(subparts.table.m))) for i in values(subparts.table.m) |> unique] |> Dict

    source_pks = [k for k in keys(subparts.type.m) if subparts.type.m[k] == PK]
    source_pks = Dict([subparts.table.m[i] for i in source_pks] .=> [String(subparts.cname[v]) for v in values(source_pks)])

    source_summary = map(enumerate(source_keys)) do (idx, s)
        (
            Identifier = source_keys[s], 
            Name = source_values[idx], 
            PrimaryKey = source_pks[s],
            Type = source_types[idx],
            Fields = source_fields_count[s],
            Indegree = source_tos_count[s],
            Outdegree = source_froms_count[s]
        )
    end

    source_summary = hcat(source_summary...)
    source_summary_cols = isempty(source_summary) ? _empty_fabric_msg() : keys(source_summary[1]) .|> String |> collect
    return source_summary, source_summary_cols

end

"""
Dispatch on `DataFabric` type to pretty print a summary of the sources within a `DataFabric`.
"""
function Base.show(io::IO, fabric::DataFabric) 
    source_summary, source_summary_cols = _source_summary(fabric)
    pretty_table(source_summary; header = source_summary_cols)
end
