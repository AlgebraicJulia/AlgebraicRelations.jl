module SQLiteTest

using ACSets
using Catlab 
using AlgebraicRelations

using Test
using DataFrames
using SQLite, DBInterface

@present SchClass(FreeSchema) begin
    Name::AttrType
    Class::Ob
    subject::Attr(Class, Name)
end
@acset_type Class(SchClass)
classes = Class{Symbol}()

class_db = DBSource(SQLite.DB(), acset_schema(classes))
execute!(class_db, "create table `Class` (_id int, subject varchar(255))")

@test columntypes(class_db) == Dict([:_id => Integer, :subject => String])

x = add_part!(class_db, :Class, [(_id=1, subject="Chemistry"), (_id=2, subject="Physics")])
@test isempty(x)

@test subpart(class_db, :subject) == DataFrame(_id=[1,2], subject=["Chemistry", "Physics"])

@test incident(class_db, [:Physics, :Chemistry], :subject) == DataFrame(_id = [1,2])

x = add_part!(class_db, :Class, (_id=3, subject="Math"))
@test isempty(x)

@test Set(subpart(class_db, :subject).subject) == Set(["Math", "Physics", "Chemistry"]) 

# TODO API isn't great but it works!
set_subpart!(class_db, :Class, [(_id=3, subject="Mathematics")]; wheres = WhereClause(:(=), :_id => 3))

@test Set(subpart(class_db, :subject).subject) == Set(["Mathematics", "Physics", "Chemistry"])

end
