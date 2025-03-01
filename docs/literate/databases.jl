using Revise
using ACSets
using Catlab
using Catlab.Graphs
using AlgebraicRelations
#
using FunSQL
using DBInterface
using MLStyle
using DataFrames

using MySQL # loads extension

# Let's establish a connection to MariaDB.
# TODO how can we make this dynamic
conn = DBInterface.connect(MySQL.Connection, "localhost", "mysql", db="acsets", 
                    unix_socket="/var/run/mysqld/mysqld.sock")


# Let's invoke an ACSet. We can think of an ACSet as a database schema with tables (objects), foreign key constraints (homs), and columns (Attrs) with types (AttrTypes).
@present SchWeightedLabeledGraph <: SchLabeledGraph begin
    Weight::AttrType
    weight::Attr(E,Weight)
end
@acset_type WeightedLabeledGraph(SchWeightedLabeledGraph, index=[:src, :tgt]) <: AbstractLabeledGraph
g = erdos_renyi(WeightedLabeledGraph{Symbol,Float64}, 5, 0.25);
g[:, :label] = Symbol.(floor.(rand(nv(g)) * nv(g)));
g[:, :weight] = floor.(rand(ne(g)) .* 100);

# Inspect ACSet
g

# We'd like the ACSet to mirror. Let's virtualize an ACSet. This is a new object that sustains a relationship between our database and our ACSet. 
vas = VirtualACSet(conn) # TODO dump into StructACSet

execute!(vas, ShowTables())

# Right now, it does not verify that the database agrees with the ACSet. We would need to `diff` ACSets.
subpart(vas, :V)

# This lets us build a lot of insert statements. We join them together.
i = join(FunSQL.render.(Ref(vas), ACSetInsert(vas, g)), " ")

execute!.(Ref(vas), ACSetInsert(vas, g))

nparts(vas, :V)

maxpart(vas, :V)

subpart(vas, :V)

subpart(vas, :E)

# can we consume the query object
incident(vas, nparts(vas, :V).count[1], :tgt)

subpart(vas, 4, :label)

# TODO move "LastRowId" to a type so we can dispatch on it
add_part!(vas, :V, (_id = only(maxpart(vas, :V).max) + 1, label = "rhombus")) 

rem_part!(vas, :V, 1)

# XXX doesn't return 
add_part!(vas, :V, (_id=1, label="spagumbus"))

rem_part!(vas, :E, 10)

subpart(vas, :V)

# TODO should be able to pass in Julia expressions for conditions like `where`
set_subpart!(vas, :V, [(label=0,)]; wheres=WhereClause(:in, :_id => [1]))

subpart(vas, 5, :_id)
subpart(vas, 1, :tgt)
subpart(vas, [1,2], :tgt)

execute!(vas, s1)

i=Insert(:Persons, [(PersonID=1, LastName="Last", FirstName="First", Address="a", City="b")])
execute!(vas, i)

u=Update(:Persons, [(LastName="First", FirstName="Last")], WhereClause(:in, :PersonID => [1]))
tostring(conn, u)

ForeignKeyChecks(conn, tostring(conn, i))

# want just a diagram of the 
h = WeightedLabeledGraph{DataFrame, DataFrame}()

v = subpart(vas, :V)
e = subpart(vas, :E)

add_part!(h, :V, label = v)
add_part!(h, :E, weight = e)
