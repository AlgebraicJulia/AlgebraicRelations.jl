using AlgebraicRelations.DB
using SQLite

@present WorkplaceSchema <: TheorySQL begin
  # Data tables
  employee::Ob
  emp_data::Attr(employee, Int64)
  
  name::Ob
  name_data::Attr(name, String)
  
  salary::Ob
  sal_data::Attr(salary, Float64)
  
  # Relation tables
  manager::Ob
  emplm::Hom(manager, employee)
  manag::Hom(manager, employee)
  
  full_name::Ob
  empln::Hom(full_name, employee)
  namen::Hom(full_name, name)
  
  income::Ob
  empli::Hom(income, employee)
  sali::Hom(income, salary)
end;   

Workplace = SchemaType(WorkplaceSchema)
f = Workplace()

db = SQLite.DB()
splt_stmts = split(generate_schema_sql(f), "\n")

@testset "Generate DB Schema" begin
  for stmt in splt_stmts
    @test DBInterface.execute(db, stmt) isa SQLite.Query
  end
end
