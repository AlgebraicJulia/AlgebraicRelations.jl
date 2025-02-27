using Catlab
using ACSets
using AlgebraicRelations

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

# given an ACSet
sch = SQLSchema(SchJunct)
tab = SQLTable(sch) # TODO this is problematic. not the right type
c = SQLCatalog(values(tab)...)

to_funsql(select, sch)

unique(rvrv(
    [incident(d, Vector{Int64}(filter(!isnothing, infer_states(d))), :src),
     # d.src in [filter(!isnothing, infer_states(d))]
     incident(d, d[:res], :src), # d.src == d[res]
     incident(d, d[:sum], :src), # d.src == d[sum]
     incident(d,
              # `d`.`
              d[collect(Iterators.flatten(
                                          # from `d` where d.op1 ∈ black_list select `_id`
                                          incident(d, collect(black_list), :op1)
                                         )), :tgt], 
        :src)]))
# ... select distinct d.id

q = From(◊Ob(:Op1)) |>
Where(:src, :∈, infer_states(d)) |> # infer states
Where(:src, :∈, d[:res] ∪ d[:sum]) |> # res
Where(:src, :∈, From(◊Ob(:Op1)) |>
          Where(:op1, :∈, :Δ) |>
          Select(◊Ob(:tgt)))

