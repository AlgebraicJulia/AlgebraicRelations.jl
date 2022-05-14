module SQLiteTest
  using Test
  using Catlab.CategoricalAlgebra
  using Catlab.Present
  using SQLite
  using AlgebraicRelations.Schemas
  using AlgebraicRelations.SQLiteInterop

  @present Business(FreeSchema) begin
    (val!Salary, Name)::AttrType
    (Employee, Manager, Income, Salary)::Ob
    name::Attr(Employee, Name)

    (man!employee, man!manager)::Hom(Manager, Employee)

    inc!employee::Hom(Income, Employee)
    inc!salary::Hom(Income, Salary)

    sal!salary::Attr(Salary, val!Salary)
  end

  busSchema = pres2schema(Business; types = Dict(:val!Salary => Float64, :Name => String))

  db = SQLite.DB()
  splt_stmts = split(render_schema(busSchema), "\n")

  @testset "Generate DB Schema" begin
    for stmt in splt_stmts
      @test DBInterface.execute(db, stmt) isa SQLite.Query
    end
  end

  reconst_stmts = split(render_schema(db2schema(db)), "\n")
  @test all(sort(splt_stmts) .== sort(reconst_stmts))
end