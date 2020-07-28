# here is an example
using Catlab, Catlab.Theories, Catlab.Present
using AlgebraicRelations.QueryLib, AlgebraicRelations.SQL,
      AlgebraicRelations.Interface;
using SQLite
using DataFrames

full_name = Ob(FreeBicategoryRelations, :full_name);
person = Ob(FreeBicategoryRelations, :person);
F = Ob(FreeBicategoryRelations, :F);
ID = Ob(FreeBicategoryRelations, :ID);

names = Hom(:names, person, full_name);
employees = Hom(:employees, person, ID);
customers = Hom(:customers, person, ID);
manager = Hom(:manager, person, person);
salary = Hom(:salary, person, F);
relation = Hom(:relation, person⊗person, F);

types  = Dict(:full_name => ([],[String]),
        :person    => ([], [Int]),
        :F         => ([], [Float64]),
        :ID        => ([], [Int]))

# Tables -> Column names
tables = Dict(:names     => (["person"], ["full_name"]),
        :employees => (["person"],["ID"]),
        :customers => (["person"],["ID"]),
        :manager   => (["person"],["manager"]),
        :salary    => (["person"],["salary"]),
        :relation  => (["person1", "person2"], ["relationship"]))

# Fill the table
syntax_types  = [full_name, person, F, ID]
syntax_tables = [names, employees, customers, manager, salary, relation]
schema = to_presentation(syntax_types, syntax_tables)

db = SQLite.DB()
splt_stmts = split(sql(types, tables, schema), "\n")

# Filter out comments which don't error in PostgresSql
filter!(l->l[1] != '-', splt_stmts)

@testset "Generate DB Schema" begin
  for stmt in splt_stmts
    @test DBInterface.execute(db, stmt) isa SQLite.Query
  end
end

# Fill out the table
insert_stmts = ["INSERT INTO employees VALUES (1, 1);",
                "INSERT INTO employees VALUES (2, 2);",
                "INSERT INTO employees VALUES (3, 3);",
                "INSERT INTO employees VALUES (4, 4);",
                "INSERT INTO customers VALUES (1, 5);",
                "INSERT INTO customers VALUES (5, 6);",
                "INSERT INTO names     VALUES (1, 'Alice Smith');",
                "INSERT INTO names     VALUES (2, 'Bob Jones');",
                "INSERT INTO names     VALUES (3, 'Eve Johnson');",
                "INSERT INTO names     VALUES (4, 'John Doe');",
                "INSERT INTO names     VALUES (5, 'Jane Doe');",
                "INSERT INTO manager   VALUES (1, 1);",
                "INSERT INTO manager   VALUES (2, 1);",
                "INSERT INTO manager   VALUES (3, 4);",
                "INSERT INTO manager   VALUES (4, 1);",
                "INSERT INTO salary    VALUES (1, 150000);",
                "INSERT INTO salary    VALUES (2, 50000);",
                "INSERT INTO salary    VALUES (3, 80000);",
                "INSERT INTO salary    VALUES (4, 90000);"]

for stmt in insert_stmts
  DBInterface.execute(db, stmt)
end

@testset "SQL From @program" begin
  # This should get everyone whose manager is their own manager
  f = @program schema (p::person) begin
    m = manager(p)
    m1 = manager(m)
    dcounit{person}(m,m1)
    return names(p)
  end

  qp = Query(types, tables, f)
  A = DBInterface.execute(db, sql(qp)) |> DataFrame
  @test A isa DataFrame
  @test ["Alice Smith",
         "Bob Jones",
         "John Doe"] == A[!,"full_name"]

  # This should get the name of each person and the salary of their manager
  f = @program schema (p::person) begin
    return names(p), salary(manager(p))
  end

  qp = Query(types, tables, f)
  A = DBInterface.execute(db, sql(qp)) |> DataFrame
  @test A isa DataFrame
  @test ["Alice Smith",
         "Bob Jones",
         "Eve Johnson",
         "John Doe"] == A[!,"full_name"]

  @test [ 150000.0,
          150000.0,
          90000.0,
          150000.0] == A[!,"salary"]
end

@testset "SQL From Formula" begin
  # Formula Method Tests

  # This should get everyone whose manager is their own manager
  f = Δ(person)⋅((manager⋅Δ(person)⋅(id(person)⊗manager)⋅dcounit(person))⊗names)

  qp = Query(types, tables, f)
  A = DBInterface.execute(db, sql(qp)) |> DataFrame
  @test A isa DataFrame
  @test ["Alice Smith",
         "Bob Jones",
         "John Doe"] == A[!,"full_name"]

  # This should get the name of each person and the salary of their manager
  f = Δ(person)⋅(names⊗(manager⋅salary))

  qp = Query(types, tables, f)
  A = DBInterface.execute(db, sql(qp)) |> DataFrame
  @test A isa DataFrame
  @test ["Alice Smith",
         "Bob Jones",
         "Eve Johnson",
         "John Doe"] == A[!,"full_name"]

  @test [ 150000.0,
          150000.0,
          90000.0,
          150000.0] == A[!,"salary"]
end

@testset "SQL From Relation" begin
  # Relation Method Tests

  # This should get everyone whose manager is their own manager
  f = @relation (p, n) where (p::person, n::full_name, m::person, m1::person) begin
    manager(p,m)
    manager(m, m1)
    manager(m1, m)
    names(p, n)
  end

  qp = Query(types, tables, f)
  A = DBInterface.execute(db, sql(qp)) |> DataFrame
  @test A isa DataFrame
  @test ["Alice Smith",
         "Bob Jones",
         "John Doe"] == A[!,"full_name"]

  # This should get the name of each person and the salary of their manager
  f = @relation (p, n, s) where (p::person, n::full_name, s::F, m::person) begin
    manager(p,m)
    salary(m,s)
    names(p,n)
  end

  qp = Query(types, tables, f)
  A = DBInterface.execute(db, sql(qp)) |> DataFrame
  @test A isa DataFrame
  @test ["Alice Smith",
         "Bob Jones",
         "Eve Johnson",
         "John Doe"] == A[!,"full_name"]

  @test [ 150000.0,
          150000.0,
          90000.0,
          150000.0] == A[!,"salary"]
end
