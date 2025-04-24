using Test

@testset "Schemas" begin
  include("schemas/BusinessSchema.jl")
end

@testset "SQLite" begin
  include("SQLiteInterop.jl")
end
