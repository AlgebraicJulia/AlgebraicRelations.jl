using Test

#@testset "AlgebraicRelations" begin
#  include("AlgebraicRelations.jl")
#end

@testset "ACSetDB" begin
  include("ACSetDB.jl")
end

@testset "ACSetQueries" begin
  include("ACSetQueries.jl")
end
