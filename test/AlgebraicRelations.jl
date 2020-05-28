# here is an example
using Catlab, Catlab.Doctrines, Catlab.Present
using Schema.QueryLib, Schema.Presentation
using Schema.Interface
import Schema.Presentation: Schema, sql

# Define the Types
Name = Ob(FreeBicategoryRelations, (:full_name, (first=String, last=String)))
Person = Ob(FreeBicategoryRelations, (:person, (id=Int,)))
X = Ob(FreeBicategoryRelations, (:X, Int))
F = Ob(FreeBicategoryRelations, (:F, Float64))
ID = Ob(FreeBicategoryRelations, (:ID, (id=Int,)))

# Define the relationships
name = Hom((name=:names, fields=("person", "full_name")), Person, Name)
emply = Hom((name=:employees, fields=("person", "ID")), Person, ID)
custo = Hom((name=:customers, fields=("person", "ID")), Person, ID)
manag = Hom((name=:manager, fields=("person", "manager")), Person, Person)
salry = Hom((name=:salary, fields=("person", "salary")), Person, F)
e_cust_conn = Hom((name=:interactions, fields=(["employee", "customer"], "interaction")), Person⊗Person, X)

# Set up arrays of types and relationships for Schema
types = [Name, Person, X,F,ID]
rels = [name, emply, custo, manag, salry, e_cust_conn]

schema = Schema(types, rels)

# Generate the Schema
prim, tab = sql(schema)
println("Copy the following to generate a database:")
println(join(prim,"\n"))
println(join(tab, "\n"))

# Fill table with testing information

println("Copy the following insertion statements to fill the table:")
println("INSERT INTO employees VALUES (ROW(1), ROW(1));")
println("INSERT INTO employees VALUES (ROW(2), ROW(2));")
println("INSERT INTO employees VALUES (ROW(3), ROW(3));")
println("INSERT INTO employees VALUES (ROW(4), ROW(4));")
println("INSERT INTO customers VALUES (ROW(1), ROW(5));")
println("INSERT INTO customers VALUES (ROW(5), ROW(6));")
println("INSERT INTO names     VALUES (ROW(1), ROW('Alice', 'Smith'));")
println("INSERT INTO names     VALUES (ROW(2), ROW('Bob', 'Jones'));")
println("INSERT INTO names     VALUES (ROW(3), ROW('Eve', 'Johnson'));")
println("INSERT INTO names     VALUES (ROW(4), ROW('John', 'Doe'));")
println("INSERT INTO names     VALUES (ROW(5), ROW('Jane', 'Doe'));")
println("INSERT INTO manager   VALUES (ROW(1), ROW(1));")
println("INSERT INTO manager   VALUES (ROW(2), ROW(1));")
println("INSERT INTO manager   VALUES (ROW(3), ROW(4));")
println("INSERT INTO manager   VALUES (ROW(4), ROW(1));")
println("INSERT INTO salary    VALUES (ROW(1), 150000);")
println("INSERT INTO salary    VALUES (ROW(2), 50000);")
println("INSERT INTO salary    VALUES (ROW(3), 80000);")
println("INSERT INTO salary    VALUES (ROW(4), 90000);")

# Generate and display a query to get (names, salaries)
println("Copy the following to generate run the query:")

#Salary and manager's name for each person
#formula = dagger(name)⋅mcopy(Person)⋅(salry⊗(manag⋅name))⋅σ(F,Name)

# Employees who have the same salary and manager
#formula = dagger(name)⋅mcopy(Person)⋅((salry⋅dagger(salry))⊗(manag⋅dagger(manag)))⋅mmerge(Person)⋅name

# Customer/employee relationship between employee and their manager
formula = mcopy(Person)⋅(id(Person)⊗manag)⋅e_cust_conn
query(f) = to_sql(make_query(schema, f))

println(query(formula))

#conn = Connection("dbname=e3isd")
#statement = prepare(conn, schema, formula)
#println(execute(conn, schema, formula))
#println(execute(statement, ["ROW(1)"]))
# get the salary of a person's manager
# query(manag⋅salry) == "select (manager.id, salary.salary) from manager join salary on manager.manager == salary.id"
