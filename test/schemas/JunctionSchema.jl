module JunctionSchemaTest

using Catlab
using Catlab.CategoricalAlgebra
using Catlab.Graphics
using AlgebraicRelations

using Test
using DataFrames
using SQLite
using FunSQL: render, SQLDialect

junction_schema = @present SchJunct(FreeSchema) begin
    Name::AttrType
    (Student, Class, Junct)::Ob
    name::Attr(Student, Name)
    subject::Attr(Class, Name)
    student::Hom(Junct,Student)
    class::Hom(Junct,Class)
end
@acset_type Junct(SchJunct)
junction = Junct{Symbol}()

schema = SQLSchema(SchJunct; types = Dict(:Name => String))

# TODO consume SQLSchema
stmts = split(render_schema(schema), "\n")
db = DBSource(SQLite.DB(), acset_schema(junction))

fabric = DataFabric()
db_id = add_source!(fabric, db)
reflection = reflect!(fabric)

@testset "Generate DB Schema" begin
    for stmt in stmts
        @test execute!(fabric, 1, stmt) isa DataFrame
    end
end

end
