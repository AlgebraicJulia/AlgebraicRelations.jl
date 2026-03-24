using AlgebraicRelations
using Catlab

using Test
using FunSQL
using SQLite
using DataFrames

@present SchoolSystem(FreeSchema) begin
    Name::AttrType
    (School, SchoolClass, Class, ClassStudent, Student)::Ob
    sc_school::Hom(SchoolClass, School)
    sc_class::Hom(SchoolClass, Class)
    cs_class::Hom(ClassStudent, Class)
    cs_student::Hom(ClassStudent, Student)
    school::Attr(School, Name)
    class::Attr(Class, Name)
    student::Attr(Student, Name)
end
schema = SQLSchema(SchoolSystem)
split_stmts = split(render_schema(schema), "\n")

bare_db = SQLite.DB()
fundb = FunSQL.DB(bare_db, catalog=FunSQL.reflect(bare_db))

old_catalog = fundb.catalog
for stmt in split_stmts
    DBInterface.execute(fundb, stmt)
end

new_catalog = fundb.catalog
@assert new_catalog == old_catalog

db = DBSource(SQLite.DB(), schema)
τ = trait(db)
for stmt in split_stmts
    execute![τ](db, stmt)
end
# TODO
# execute!(db, ShowTables())

insert_stmts = [
    "INSERT OR IGNORE INTO Student (Student_id, student) VALUES (1, 'Gregorio')",
    "INSERT OR IGNORE INTO Student (Student_id, student) VALUES (2, 'Heather')",
    "INSERT OR IGNORE INTO School (School_id, school) VALUES (1, 'Erewhon University');",
    "INSERT OR IGNORE INTO School (School_id, school) VALUES (2, 'Utopia State');",
    "INSERT OR IGNORE INTO ClassStudent (ClassStudent_id, cs_class, cs_student) VALUES (1, 1, 1);",
    "INSERT OR IGNORE INTO ClassStudent (ClassStudent_id, cs_class, cs_student) VALUES (1, 2, 1);",
    "INSERT OR IGNORE INTO Class (Class_id, class) VALUES (1, 'math');",
    "INSERT OR IGNORE INTO Class (Class_id, class) VALUES (2, 'science');", 
    "INSERT OR IGNORE INTO SchoolClass (SchoolClass_id, sc_school, sc_class) VALUES (1, 1, 1);"
]

for stmt in insert_stmts
    execute![τ](db, stmt)
end

subpart(db, :Class)


fabric = DataFabric() 
m = add_source!(fabric, db)

reflect!(fabric)

@test subpart(fabric, :Class) == [1,2]
@test subpart(fabric, :ClassStudent) == [1]
@test subpart(fabric, :student) == ["Gregorio", "Heather"]
