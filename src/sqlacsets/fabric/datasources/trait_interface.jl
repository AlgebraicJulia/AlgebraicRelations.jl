# # notes on how to set up interfaaces for the 3 concrete data sources

@interface ThDataSource begin
    @import Nothing::TYPE
    @import Vector::TYPE
    Column::TYPE 
    Row::TYPE # PartId
    DataSource::TYPE # Type of data 

    reconnect!(d::DataSource)::DataSource
    incident(d::DataSource, c::Column, r::Row)::Vector{Row}
    execute!(d::DataSource)::Vector{Row}
end

struct InMemoryTrait end
inmemory_trait = InMemoryTrait()

struct DBSourceTrait end
dbsource_trait = DBSourceTrait()

@instance ThDataSource{Symbol, Int, DataSource=InMemory} [model::InMemoryTrait] begin 
    reconnect!(m::InMemory)::InMemory = m
    execute!()::Vector{Int} = Int[]
    function incident(m::InMemory, colname::Symbol, partid::Int)::Vector{Int}
        incident(m.value, colname, partid)
    end
end

@instance ThDataSource{Symbol, Int, DataSource=DBSource} [model::DBSourceTrait] begin 
    function reconnect!(source::DBSource)::DBSource
        source.conn = FunSQL.DB(source.conn.raw, catalog=FunSQL.reflect(source.conn.raw))
        source
    end
    execute!()::Vector{Int} = Int[]
    # incident(colname::Symbol, partid::Int)::Vector{Int} = 
    #     incident(model, colname, partid)
end

# each submodule should implement the instance and the interface is defined here

# @instance ThDatasource{AbstractString, Int} [model::DatabaseDS] begin 
#     function reconnect!()::Nothing
#         source.conn = FunSQL.DB(source.conn.raw, catalog=reflect(source.conn.raw))
#     end

#     function incident(colname::AbstractString, rowid::Int)::Vector{}
#         query = FROM(tablecolumn.first) |> WHERE(FUN(:in, tablecolumn.second, vals...)) |> SELECT(:_id)
#         df = DBInterface.execute(db.conn, query) |> DataFrames.DataFrame
#     end

# end

# g = path_graph(Graph, 7)

# incident[g](:src, 3) # incident(Trait(g), :src, 3) = incident(g, :src, 3)
