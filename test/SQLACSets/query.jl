using Test

using Catlab
using ACSets
using AlgebraicRelations

@present SchJunct(FreeSchema) begin
    Name::AttrType
    Student::Ob
    name::Attr(Student, Name)
    Class::Ob
    subject::Attr(Class, Name)
    Junct::Ob
    student::Hom(Junct,Student)
    class::Hom(Junct,Class)
end
@acset_type JunctionData(SchJunct, index=[:name])
jd = JunctionData{Symbol}()
#
df = Dict(:Fiona => [:Math, :Philosophy, :Music],
          :Gregorio => [:Cooking, :Math, :CompSci],
          :Heather => [:Gym, :Art, :Music, :Math])
#
foreach(keys(df)) do student
    classes = df[student]
    # let's make this idempotent by adding student only if they aren't in the system
    student_id = incident(jd, student, :name)
    if isempty(student_id); student_id = add_part!(jd, :Student, name=student) end
    # for each of the classes the student has...
    foreach(classes) do class
        # idempotently add their class
        class_id = incident(jd, class, :subject)
        if isempty(class_id); class_id = add_part!(jd, :Class, subject=class) end
        # enforce pair constraint
        id_pair = incident(jd, only(student_id), :student) ∩ incident(jd, only(class_id), :class)
        isempty(id_pair) && add_part!(jd, :Junct, student=student_id, class=only(class_id))
    end
end

q = SQLACSetNode(:Junct; cond=[(:student, :∈, 1), (:class, :∈, 2)])
@test q(jd) == [1,2,3]
