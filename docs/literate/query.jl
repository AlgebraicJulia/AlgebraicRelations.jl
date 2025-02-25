@present SchJunct(FreeSchema) begin
    Name::AttrType
    #
    Student::Ob
    name::Attr(Student, Name)
    #
    Class::Ob
    subject::Attr(Class, Name)
    #
    Junct::Ob
    student::Hom(Junct,Student)
    class::Hom(Junct,Class)
end

@acset_type JunctionData(SchJunct, index=[:name])
jd = JunctionData{Symbol}()

df = Dict(:Fiona => [:Math, :Philosophy, :Music],
          :Gregorio => [:Cooking, :Math, :CompSci],
          :Heather => [:Gym, :Art, :Music, :Math])

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

import Tables
map(Tables.rowcount, tables(jd))

using DataFrames

pretty_tables(jd, crop=:both, display_size=(15,80))

select = @relation (student=student, class=class, name=name, subject=subject) begin
    Student(_id=student, name=name)
    Class(_id=class, subject=subject)
    Junct(student=student, class=class)
end
res=query(jd, select, table_type=DataFrame)

FROM Junct JOIN Student ON Junct.student = Student._id 

# given an ACSet
sch = SQLSchema(SchJunct)
tab = SQLTable(sch) # TODO this is problematic. not the right type
c = SQLCatalog(values(tab)...)

to_funsql(select, sch)

# TODO convert SQLNode to RelationalDiagram
# q1 = tables(g) |> FROM |> SELECT(*)
q2 = tables(g) |> From

using FunSQL
using FunSQL: SQLCatalog, render, reflect
using FunSQL: From, Group, Select, Agg

q = From(:person) |>
    Group() |>
    Select(Agg.count())

    conn = DBInterface.connect(g)


c = FunSQL.DB(conn, catalog=reflect(conn))

render(conn, q) 

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

# from d where
#   d.src ∈ filter(!isnothing, infer_states(d)) ||
#   d.src ∈ d[:res] ||
#   d.src ∈ d[:sum] ||
#   d.src ∈ d[(from d where d.op1 ∈ blacklist select d.id), :tgt]
# select distinct d.id

s = ACSetSelect(:E; what=SelectColumns(:E => :_id), 
            on=nothing, 
            wheres=WhereClause(:OR, [
                WhereClause(:in, :src => filter(!isnothing, [1])),
                WhereClause(:in, :src => [2]), # d[:res]
                WhereClause(:in, :src => [2]), # d[:sum]
    WhereClause(:in, :src => begin
                    ids = ACSetSelect(:V;
                                      what=SelectColumns(:V => :_id),
                                      on=nothing,
                                      wheres=WhereClause(:in, :op1 => [1] # blacklist
                                                        ))
                    [1] # d[ids, :tgt]
                end)]))

parts(g, :E)[g[:src] .== filter(!isnothing, [1]) .&& 
             g[:src] .== [4] .&&
             g[:src] .== [2]
            ]
