# # Data Fabric

using Catlab
using ACSets
using AlgebraicRelations
# SQLACSets are data sources which implement the ACSet interface. The trivial
# example of this case is the ACSet, which is an in-memory database. Using
# ACSets, we can connect to form a database schema with multiple data soruces, 
# or a "data mesh."

# Data meshes are step in a sequence of data management architectures for handling
# different commericial, industrial, and scientific requirements for data
# interconnectivity. The simplest paradigm is a "data store," which may be just a single database operating at a central location. Then we have a "data warehouse," then a "data lake," and then "data meshes" and "data fabrics."

# We can go one step further by defining a "data fabric," a data mesh which
# implements a virtualization layer for unified access. In commercial
# applications, a data fabric retrieves data from each source into memory to
# allow for faster queries and a unified access protocol.

# We have implemented the "data mesh" level of the data fabric. That is, we do
# not load data from our data sources into memory as a unified schema.
# Currently, we rely on our **catalog** to both direct us to the right database
# connection for our query as well as keep a record of the available
# information. But as we transition into loading subsets of data into memory,
# it's worthwhile to explore whether a separate graph-like object would be
# responsible for retaining the actual queried data.

# An already-existing distinction in AlgebraicJulia is that between
# a presentation of a schema and its instantiation.

# In the meantime, let's invoke our data fabric.
fabric = DataFabric()

# We will assume we have a list of students schematized...
@present SchStudent(FreeSchema) begin
    Name::AttrType
    Student::Ob
    name::Attr(Student, Name)
end
@acset_type Student(SchStudent)
students = InMemory(Student{Symbol}())

# ...and their classes...
@present SchClass(FreeSchema) begin
    Name::AttrType
    Class::Ob
    subject::Attr(Class, Name)
end
@acset_type Class(SchClass)
classes = Class{Symbol}()

using SQLite, DBInterface
class_db = DBSource(acset_schema(classes), SQLite.DB())

execute!(class_db, "create table `Class` (_id int, subject varchar(255))")

# ...but they are stored in different data sources. Let's suppose we have
# a many-many relationship of students and classes. Here is their membership:
df = Dict(:Fiona => [:Math, :Philosophy, :Music],
          :Gregorio => [:Cooking, :Math, :CompSci],
          :Heather => [:Gym, :Art, :Music, :Math])

# Let's construct an example where the students and class information is stored
# elsewehere and the membership is currently unknown. We'll add students...
add_parts!(students, :Student, length(keys(df)), name=keys(df))
# TODO implement pass-through method

subpart(students.value, :name)

# ...and classes...
execute!(class_db,
    """insert or ignore into `class` (_id, subject) values
    (1, "Math"), (2, "Philosophy"), (3, "Music"),
    (4, "Cooking"), (5, "CompSci"), (6, "Gym"), (7, "Art"),
    (8, "Music")
    """)
subpart(class_db, :class) # TODO notice how we don't query by column. 

# We will reconcile them locally with a junction table that has a reference to them, schematized as simply a "Junction" object. Since we are not yet ready to add constraints to both Student and Class, the Junction schema--essentially a table of just references--is very plain.
@present SchSpan(FreeSchema) begin
    Junction::Ob
end
@acset_type JunctStudentClass(SchSpan)

# We'll gradually adapt this example to different kinds of data sources, but
# for the time being we'll consider both student and class tables as
# in-memory data sources.
add_source!(fabric, students)
add_source!(fabric, class_db)
add_source!(fabric, InMemory(JunctStudentClass()))

add_fk!(fabric, 3, 1, :student => :Student)
add_fk!(fabric, 3, 2, :class => :Class)

# The DSG describes three data sources with two constraints. 
fabric.graph

# Whether the constraints are valid is not yet enforced...they're just something we the users assert. To assure ourselves that this schema makes sense, we should be able to adapt our `join` method from Catlab to recobble the familiar Student-Class junction example. Because the data fabric presents a unified access layer for data, we'd need a catalog of available schema to find the information we need. In database science, reflection is the ability for databases to store information about their own schema. The fact that information about a database schema can also be represented as a schema is more plainly attributed to the mathematical formalism of schemas as attributed C-Sets. So naturally we implemented `reflect` for the data fabric:
reflect!(fabric)

# The populated catalog
fabric.catalog

# Let's query the names of the students and the available classes. The names of
# the students are stored in-memory:
subpart(fabric, :name)

# Meanwhile the available subjects are stored in a SQLite database. We query
# them as if they were an ACSet.
subpart(fabric, :subject)

# What are the ID
incident(fabric, :Philosophy, :subject)

#  
incident(fabric, :Heather, :name)


