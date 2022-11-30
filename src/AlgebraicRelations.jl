module AlgebraicRelations
  using Requires

  include("Schemas.jl")
  include("Queries.jl")

  function __init__()
    @require SQLite="0aa819cd-b072-5ff4-a722-6bc24af294d9" include("SQLiteInterop.jl")
  end
end
