module Formatter

using Dates

struct QueryResult{T}
    result::T
    dt::DateTime
    function QueryResult(x::T) where T
        new{T}(x, now())
    end
end

abstract type AbstractResultFormatter end

struct DFQueryFormatter! <: AbstractResultFormatter end

(qf::DFQueryFormatter!)(r::QueryResult) = r

end
