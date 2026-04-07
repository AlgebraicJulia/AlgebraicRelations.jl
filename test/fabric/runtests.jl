@testset "SQLite" begin
include("datasources/sqlite.jl")
end

@testset "Chained Accessors" begin
include("chained_accessors.jl")
end

@testset "Reflection" begin
include("reflection.jl")
end

@testset "Single DB" begin
include("single_db.jl")
end
