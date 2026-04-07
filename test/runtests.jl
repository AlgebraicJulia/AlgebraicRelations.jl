using Test

@testset "Schemas" begin
  include("schemas/BusinessSchema.jl")
  include("schemas/JunctionSchema.jl")
end

@testset "Fabric" begin
    include("fabric/runtests.jl") 
end
