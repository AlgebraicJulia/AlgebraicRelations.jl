# TODO GraphQL?

using Test

using AlgebraicRelations
using Catlab
#
using FunSQL
using SQLite
using DataFrames

# Let's define our schema
@present Business(FreeSchema) begin
  (val!Salary, Name)::AttrType
  (Employee, Manager, Income, Salary)::Ob
  name::Attr(Employee, Name)
  #
  (man!employee, man!manager)::Hom(Manager, Employee)
  #
  inc!employee::Hom(Income, Employee)
  inc!salary::Hom(Income, Salary)
  #
  sal!salary::Attr(Salary, val!Salary)
end

# this is just for the purposes of creating tables
busSchema = SQLSchema(Business; types = Dict(:val!Salary => Float64, :Name => String))
split_stmts = split(render_schema(busSchema), "\n")

# let's start by understanding the FunSQL interface.
bare_db = SQLite.DB()
fundb = FunSQL.DB(bare_db, catalog=FunSQL.reflect(bare_db))

old_catalog = fundb.catalog
for stmt in split_stmts
    DBInterface.execute(fundb, stmt)
end

# the catalog is the same
new_catalog = fundb.catalog
@assert new_catalog == old_catalog

# OurSQL

# Let's define a data source for it. We automatically wrap a FunSQL.DB over it.
db = DBSource(SQLite.DB())

# we have our own `execute!` 
for stmt in split_stmts
    execute!(db, stmt)
end
recatalog!(db)

# ...
execute!(db, ShowTables())

# data fabric
fabric = DataFabric()

busSchema = add_to_catalog!(fabric.catalog, Business;
    source=db, types = Dict(:val!Salary => Float64, :Name => String))
# splt_stmts = split(render_schema(busSchema), "\n")

insert_stmts = [
    "INSERT OR IGNORE INTO employee (name, id)   VALUES ('Bob', 1);",
    "INSERT OR IGNORE INTO employee (name, id)   VALUES ('Alice', 2);",
    "INSERT OR IGNORE INTO employee (name, id)   VALUES ('Charlie', 3);",
    "INSERT OR IGNORE INTO employee (name, id)   VALUES ('Eve', 4);",
    "INSERT OR IGNORE INTO manager  (employee, manager, id)    VALUES (1, 1, 1);",
    "INSERT OR IGNORE INTO manager  (employee, manager, id)    VALUES (2, 1, 2);",
    "INSERT OR IGNORE INTO manager  (employee, manager, id)    VALUES (3, 4, 3);",
    "INSERT OR IGNORE INTO manager  (employee, manager, id)    VALUES (4, 1, 4);",
    "INSERT OR IGNORE INTO salary   (salary, id)    VALUES (50000, 1);",
    "INSERT OR IGNORE INTO salary   (salary, id)    VALUES (150000, 2);",
    "INSERT OR IGNORE INTO salary   (salary, id)    VALUES (80000, 3);",
    "INSERT OR IGNORE INTO salary   (salary, id)    VALUES (90000, 4);",
    "INSERT OR IGNORE INTO income   (employee, salary, id)    VALUES (1, 1, 1);",
    "INSERT OR IGNORE INTO income   (employee, salary, id)    VALUES (2, 2, 2);",
    "INSERT OR IGNORE INTO income   (employee, salary, id)    VALUES (3, 3, 3);",
    "INSERT OR IGNORE INTO income   (employee, salary, id)    VALUES (4, 4, 4);"];
for stmt in insert_stmts
    execute!(db, stmt)
end

execute!(db, ShowTables())

subpart(db, :Salary)
subpart(fabric, :Salary)

nparts(fabric, :Salary)

incident(fabric, 150000.0, (:Salary, :salary))

# MISC. IGNORE

set_subpart!(fabric.catalog, 1, :conn, db) 

# Let's add a connection to this data fabric
add_source!(fabric, db)

subpart(fabric.graph, 1, :label)

# execute a statement on the catalog
_db = subpart(fab.catalog, 1, :conn)

# TODO execute should be defined 
DBInterface.execute(_db.conn, "select * from `Salary`;") |> DataFrame
