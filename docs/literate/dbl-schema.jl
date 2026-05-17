using Catlab

# SchSpan(FreeDoubleSchema)
@present _Sch(FreeDoubleSchema) begin
    Value::AttrType
    (School, Student, Class)::Ob
    attends::Hom(Student, School)
    StudentClass::Pro(Student, Class)
    grade::Attr(⊤(StudentClass), Value)
end

# This will create a type with a many-many table. It correspnds to a model of the underlying theory of free double schema.

p = School{Grade}()


macro schema(head, body)
    dump(body)
end
