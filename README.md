# AlgebraicRelations.jl

AlgebraicRelations.jl is a Julia library built to provide an intuitive and elegant method for generating SQL queries. This package provides tooling for defining database schemas, generating query visualizations, and connecting directly up to a PostgreSQL server. This package is built on top of [Catlab.jl](https://github.com/epatters/Catlab.jl) which is the powerhouse behind its functions.

## Learning by Doing

The functions of this library may be best explained by showing various examples of how it can be used. This will be shown in the steps of [Defining a Schema](#defining-a-schema), [Creating Queries](#creating-queries), and [Connecting to PostgreSQL](#connecting-to-postgresql).

### Defining a Schema

The definition of a schema requires two parts, the syntax and the semantics.

#### Syntax
Defining syntax involves defining variable names for the different datatypes and defining the relationships between these datatypes (tables). This is done in the `Catlab.jl` framework, so these definitions look like:
```julia
# Define Types
full_name = Ob(FreeBicategoryRelations, :full_name);
person = Ob(FreeBicategotryRelations, :person);
F = Ob(FreeBicategoryRelations, :F);
ID = Ob(FreeBicategoryRelations, :ID);

# Define Tables
names = Hom(:names, person, full_name);
employees = Hom(:employees, person, ID);
customers = Hom(:customers, person, ID);
manager = Hom(:manager, person, person);
salary = Hom(:salary, person, F);
relation = Hom(:relation, person⊗person, F)
```
The above section of code defines some relationships that may be seen in the typical business.

The types are defined as objects of a [Free Bicategory of Relations](#theory) and given a `symbol` representation.

The tables are defined as relationships between these types with a `symbol` representation. The second and third arguments to the `Hom` functions are respectively the domain and codomain of the relationships. While which type is in which section is irrelevant for SQL tables, it is important when defining queries. One last thing to note is the symbol `⊗` (monoidal product) in the last `Hom` statement. This symbol joins two types, allowing for multiple types in the domain and codomain. To the database, this means nothing more than that, for the table `relation` there are two columns of type `person` and one of type `F`.

#### Semantics
Defining the semantics of the types and tables will be what translates between these relationships and the actual database. Here is where we define any composite types (if necessary) and the column names that relate to the types previously defined in the tables. The following example is the semantics for the syntax defined above:
```julia
# Data Types
types  = Dict(:full_name => (["first", "last"],[String,String]),
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
```
The above section defines two dictionaries which map from the symbols of the types and homomorphisms to the necessary database information.

The `types` variable stores a map from `symbol` to a tuple of arrays. The first array will be empty if the type can be represented as a native SQL datatype. If the type must be a composite datatype, then the first array will hold the names of the fields of the composite datatype (e.g. `["first", "last"]` for composite type `full_name`). The second array will either the types that correspond with the names in the first array, or the primitive type. If the desired SQL type isn't able to be converted from a Julia type, then the second array can also contain `String`s with the SQL type name.

The `tables` variable stores the names for the domain and codomain columns for each `Hom` defined before. If there were any monoidal products (`⊗`) in the `Hom` expression, then each type in the product needs it's own name (see `:relation`).

### Creating Queries
The `Catlab.jl` backend allows these `Hom` and `Ob` expressions to be used in generating SQL queries in two ways. The first is using the syntax of `BicategoryRelations` in Catlab to write formulas describing the query. The second is to use the `program` macro made available through `Catlab.jl`

#### Algebraic Formula
Generating formulae will probably be more appropriate for users with previous experience in [Category Theory](https://en.wikipedia.org/wiki/Category_theory) (see the [Theory](#theory) section for more resources). For those who have experience with this syntax, we will give an example query below that gets, for each employee, their salary, their relationship with their manager, and their full name:
```julia
formula = Δ(person)⋅((Δ(person)⋅(salary⊗names))⊗(Δ(person)⋅(manager⊗id(person))⋅relation))
q = Query(types, tables, formula)
println(sql(q))
```
The result of running the code above is:
```SQL
SELECT t4.person AS person, t4.salary AS salary, t5.full_name AS full_name, t7.relationship AS relationship
FROM salary AS t4, names AS t5, manager AS t6, relation AS t7
WHERE t4.person=t5.person AND t4.person=t6.person AND t4.person=t7.person2 AND t6.manager=t7.person1;
```
As can be seen above, the method of generating the SQL is to first create a `Query` object from the types and tables (we defined before) and the formula. In order to get the plaintext query we run this through `sql(q)`, but we can also use the `Query` object later in our direct interactions with the PostgreSQL database.

#### Program
The programmatic method of generating queries is much closer to a kind of interface that languages like Matlab would provide. Here is an example of defining the same query as above here:

```Julia
syntax_types  = [full_name, person, F, ID]
syntax_tables = [names, employees, customers, manager, salary, relation]
schema = to_presentation(syn_types, syn_tables)

f = @program schema (p::person) begin
  m = manager(p)
  return salary(p), names(p), relation(m, p)
end
q = Query(types, tables, f)
println(sql(q))
```
This code results in the query:
```SQL
SELECT t3.person AS person, t4.salary AS salary, t5.full_name AS full_name, t6.relationship AS relationship
FROM manager AS t3, salary AS t4, names AS t5, relation AS t6
WHERE t3.person=t4.person AND t3.person=t5.person AND t3.person=t6.person2 AND t3.manager=t6.person1;
```
The first step to generate a query from a program is to generate a presentation (`schema`) of the `Ob` and `Hom` objects. We do this by creating an array of `Ob` objects and a separate array of `Hom` objects. These will define what functions and types we are able to use while inside the `@program` statement. The syntax of the `@program` statement is fairly straightforward, but there are a couple of extra features to note. First, if you want to use a wildcard as an argument to a function, just place `[]` where the argument would go. Second, if you want to enforce equality between two variables (`a` and `b`), place them in brackets (`[a,b]`).
### Connecting to PostgreSQL
The connection to PostgreSQL is fairly straightforward. We first create a connection using the [LibPQ.jl](https://invenia.github.io/LibPQ.jl/stable/) library:
```Julia
conn = Connection("dbname=test_db");
```
We then can prepare statements and run them with arguments like:
```Julia
statement = prepare(conn,q)
execute(statement, [3])
```
which will obtain all of the information from the previous query for the employee with id 3.

We can also run queries over all values like:
```Julia
execute(conn, q)
```
which will get all of the above information for all employees.

The `execute` function will return a `DataFrame` object (from the [`DataFrames.jl`](http://juliadata.github.io/DataFrames.jl/stable/) library)

## Theory
