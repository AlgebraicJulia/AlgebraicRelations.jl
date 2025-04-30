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

add_part!(class_db, :Class, (_id=1, subject="Chemistry"))

subpart(class_db, :Class)
