using AlgebraicRelations.ACSetDB
using AlgebraicRelations.ACSetQueries
using SQLite

@present WorkplaceSchema <: TheorySQL begin
  # Data tables
  employee::Ob
  emp_data::Attr(employee, Int64)
  emp_id::Attr(employee, Int64)

  name::Ob
  empn::Hom(name, employee)
  name_data::Attr(name, String)

  salary::Ob
  emps::Hom(salary, employee)
  sal_data::Attr(salary, Float64)

  # Relation tables
  manager::Ob
  emplm::Hom(manager, employee)
  manag::Hom(manager, employee)
end;

Workplace = SchemaType(WorkplaceSchema)
schema = Workplace()

db = SQLite.DB()
splt_stmts = split(generate_schema_sql(schema), "\n")

@testset "Generate DB Schema" begin
  for stmt in splt_stmts
    @test DBInterface.execute(db, stmt) isa SQLite.Query
  end
end

# Fill out the table
insert_stmts = ["INSERT INTO employee   VALUES (1, 1);",
                "INSERT INTO employee   VALUES (2, 2);",
                "INSERT INTO employee   VALUES (3, 3);",
                "INSERT INTO employee   VALUES (4, 4);",
                "INSERT INTO name       VALUES (1, 'Alice Smith');",
                "INSERT INTO name       VALUES (2, 'Bob Jones');",
                "INSERT INTO name       VALUES (3, 'Eve Johnson');",
                "INSERT INTO manager    VALUES (1, 1);",
                "INSERT INTO manager    VALUES (2, 1);",
                "INSERT INTO manager    VALUES (3, 4);",
                "INSERT INTO manager    VALUES (4, 1);",
                "INSERT INTO salary     VALUES (1, 150000);",
                "INSERT INTO salary     VALUES (2, 50000);",
                "INSERT INTO salary     VALUES (3, 80000);",
                "INSERT INTO salary     VALUES (4, 90000);"]

for stmt in insert_stmts
  DBInterface.execute(db, stmt)
end


@testset "Generate SQL Queries" begin
  q = @query schema (p, n) where (p::Int64, n::String, m::Int64, m1::Int64) begin
    manager(p,m)
    manager(m, m1)
    manager(m1, m)
    name(p, n)
  end
end
