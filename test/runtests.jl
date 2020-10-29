using Test

#@testset "AlgebraicRelations" begin
#  include("AlgebraicRelations.jl")
#end

@testset "DB" begin
  include("DB.jl")
end

@testset "Queries" begin
  include("Queries.jl")
end

@testset "Workflow" begin
  include("Workflows.jl")
end
