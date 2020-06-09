# Algebraic Relations

AlgebraicRelations.jl is a Julia library built to provide an intuitive and elegant method for generating SQL queries. This package provides tooling for defining database schemas, generating query visualizations, and connecting directly up to a PostgreSQL server. This package is built on top of [Catlab.jl](https://github.com/epatters/Catlab.jl) which is the powerhouse behind its functions.

## Learning by Doing

The functions of this library may be best explained by showing various examples of how it can be used. This will be shown in the steps of [Defining a Schema](#defining-a-schema), [Creating Queries](#creating-queries), and [Connecting to PostgreSQL](#connecting-to-postgresql).

### Defining a Schema

The definition of a schema requires two parts, the syntax and the semantics. First we'll cover how to define the syntax of a database schema. This involves defining variable names for the different datatypes and defining the relationships between these datatypes (tables). This is done in the `Catlab.jl` framework, so these definitions look like:
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

The tables are defined as relationships between these types with a `symbol` representation. The second and third arguments to the `Hom` functions are respectively the domain and codomain of the relationships. While which type is in which section is irrelevant for SQL tables, it is important when defining queries. One last thing to note is the symbol `⊗` in the last `Hom` statement. This symbol joins two types, allowing for multiple types in the domain and codomain. To the database, this means nothing more than that, for the table `relation` there are two columns of type `person` and one of type `F`.

### Creating Queries

### Connecting to PostgreSQL

## Theory
