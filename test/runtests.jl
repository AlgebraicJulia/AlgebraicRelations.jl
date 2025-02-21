using Test

@testset "Schemas" begin
  include("Schemas.jl")
end

@testset "SQLite" begin
  include("SQLiteInterop.jl")
end