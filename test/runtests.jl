using Test

# @testset "Schemas" begin
#   include("schemas/BusinessSchema.jl")
# end

# @testset "SQLite" begin
#   include("SQLiteInterop.jl")
# end

@testset "SQLACSets" begin
    include("sqlacsets/datasources/sqlite.jl")
    include("sqlacsets/chained_accessors.jl")
    include("sqlacsets/reflection.jl")
end
