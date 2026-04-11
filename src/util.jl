import Dates

const Maybe{T} = Union{T, Nothing} 

# DataFabric
struct Log
    time::Dates.DateTime
    event
    Log(event::DataType) = new(Dates.now(), event)
end
export Log
