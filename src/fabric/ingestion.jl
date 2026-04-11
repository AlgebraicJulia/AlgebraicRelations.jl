using CSV

function get_fk_type(fabric::DataFabric, col::Symbol)
    col = incident(fabric.catalog, col, :cname)
    from, = incident(fabric.catalog, col, :from)
    only(subpart(fabric.catalog, from, [:to, :type]))
end

function is_fk_column(fabric::DataFabric, col::Symbol)
    subpart(fabric.catalog, incident(fabric.catalog, col, :cname), :from) == 0
end

function ingest_csv!(fabric, table_name::Symbol, path::String)
    df = CSV.read(path, DataFrame; stringtype=String)
    for row in eachrow(df)
        kwargs = Dict{Symbol,Any}()
        for col in names(df)
            val = row[col] 
            # check if this column is an FK in the catalog
            # TODO some redundancy
            if is_fk_column(fabric, Symbol(col))
                target_type = get_fk_type(fabric, Symbol(col))
                kwargs[Symbol(col)] = FK{target_type}(val)
            else
                kwargs[Symbol(col)] = val isa String ? Symbol(val) : val
            end
        end
        add_part!(fabric, table_name; kwargs...)
    end
end
export ingest_csv!

