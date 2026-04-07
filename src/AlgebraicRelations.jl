module AlgebraicRelations

using Reexport

function trait end
export trait

include("util.jl")

# manipulating database schemas
include("sql/SQL.jl")

# include("libpq/Interface.jl")

# a fabric is a graph valued in ACSets which behaves like a database
include("fabric/Fabric.jl")

# include("Queries.jl")

@reexport using .SQL
# @reexport using .Queries
@reexport using .Fabric

function reload! end
export reload!

end
