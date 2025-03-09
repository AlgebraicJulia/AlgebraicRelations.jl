using Catlab
using ACSets
using AlgebraicRelations
# SQLACSets are data sources which implement the ACSet interface. The trivial
# example of this case is the ACSet, which is an in-memory database. Using
# ACSets, we can connect to form a database schema with multiple data soruces, 
# or a "data mesh."

# Data meshes are part of a sequence of paradigms of data management responding
# to increasing levels of commercial need for interconnectivity.

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

# ...and their classes...
@present SchClass(FreeSchema) begin
    Name::AttrType
    Class::Ob
    subject::Attr(Class, Name)
end
@acset_type Class(SchClass)

# ...but they are stored in different data sources. We will reconcile them locally with a junction table that has a reference to them, schematized as simply a "Junction" object. Since we are not yet ready to add constraints to both Student and Class, the Junction schema--essentially a table of just references--is very plain.
@present SchSpan(FreeSchema) begin
    Junction::Ob
end
@acset_type JunctStudentClass(SchSpan)

# Let's populate the Student and Class tables.
# ...TODO...

# We'll gradually adapt this example to different kinds of data sources, but
# for the time being we'll consider both student and class tables are
# in-memory. 
add_source!(fabric, InMemory(Student{Symbol}()))
add_source!(fabric, InMemory(Class{Symbol}()))
add_source!(fabric, InMemory(JunctStudentClass()))

add_fk!(fabric, 3, 1, :student => :Student)
add_fk!(fabric, 3, 2, :class => :Class)

# The DSG describes three data sources with two constraints. Whether the
# constraints are valid is not yet enforced...they're just something we the
# users assert. To assure ourselves that this schema makes sense, we should be able to adapt our `join` method from Catlab to recobble the familiar Student-Class junction example. However in many professional applications, it is a fact that data lives in many places.
