using Catlab, ACSets
using AlgebraicRelations
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

add_part!(class_db, :Class, [(_id=1, subject="Chemistry"), (_id=2, subject="Physics")])

subpart(class_db, :subject)

incident(class_db, [:Physics, :Chemistry], :subject)

add_part!(class_db, :Class, (_id=3, subject="Math"))

subpart(class_db, :subject)

@assert Set(["Math", "Physics", "Chemistry"]) == Set(subpart(class_db, :subject).subject)

# TODO API isn't great but it works!
set_subpart!(class_db, :Class, [(_id=3, subject="Mathematics")]; wheres = WhereClause(:(=), :_id => 3))

@assert Set(["Mathematics", "Physics", "Chemistry"]) == Set(subpart(class_db, :subject).subject)
