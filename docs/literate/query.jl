using Catlab
using ACSets
using AlgebraicRelations
#
using DataFrames
# Catlab.jl allows us to build conjunctive queries on ACSets with the `@relation` macro. In this example, we will show how we can specify conjunctive queries with a FunSQL-like syntax. Let's load up our student-class schema again.
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

# Let's demonstrate for ourselves that we can specify a conjunctive query with Catlab. 
select = @relation (student=student, class=class, name=name, subject=subject) begin
    Student(_id=student, name=name)
    Class(_id=class, subject=subject)
    Junct(student=student, class=class)
end
res=query(jd, select, table_type=DataFrame)

q = From(:Student) |> Select(:name);
q(jd)

q = From(:Student) |> 
    Where(:Student, From(:Junct) |> Select(:student)) |> 
    Select(:name); # XXX Selecting `Student` does not work
q(jd)

q = From(:Student) |>
Where(:Student, From(:Junct) |> Select(:student)) &
Where(:name, :Gregorio) | Where(:name, :Fiona) |> Select(:name);
q(jd)

q = From(:Student) |> Where(:name, [:Gregorio, :Fiona]) |> Select(:name);
@assert q(jd) == [:Fiona, :Gregorio]

q = From(:Student) |> Where(:name, ∉, :Gregorio) |> Select(:name);
@assert q(jd) == [:Fiona, :Heather]

isGregorio(x::Symbol) = x == :Gregorio
@assert !isGregorio(:Heather)

q = From(:Student) |> Where(:name, !isGregorio) |> Select(:name);
@assert q(jd) == [:Fiona, :Heather]
