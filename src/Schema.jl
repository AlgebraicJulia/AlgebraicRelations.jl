module Presentation
export Schema

struct Schema{T, U}
  types::Vector{T}
  relations::Vector{U}
end
end
