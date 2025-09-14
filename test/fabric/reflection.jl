using Test

using ACSets
using Catlab
using AlgebraicRelations

τ = AlgebraicRelations.Fabric.DatabaseDS.DBSourceTrait()

@testset "Wine" begin

    fabric = DataFabric()
    
    @present SchCountry(FreeSchema) begin
        Name::AttrType
        Country::Ob
        (country, code)::Attr(Country, Name)
    end
    @acset_type Country(SchCountry)
    country = InMemory(Country{Symbol}())
    country_src = add_source!(fabric, country)
    
    @present SchWinemaker(FreeSchema) begin
        (Name, Country)::AttrType
        Winemaker::Ob
        country_code::Attr(Winemaker, Country)
        wm_name::Attr(Winemaker, Name) # TODO "name" does not get entered
        # fk constraint means that there is *some* schema out there
    end
    @acset_type Winemaker(SchWinemaker)
    winemaker = InMemory(Winemaker{Symbol, FK{Country}}())
    winemaker_src = add_source!(fabric, winemaker)
    add_fk!(fabric, winemaker_src, country_src, :Winemaker!country_code => :Country!Country_id)
    
    reflect!(fabric)
    
    @test subpart(fabric.catalog, :type) == [PK, Symbol, Symbol, PK, FK{Country}, Symbol]

end

@testset "Class" begin

    fabric = DataFabric()

    @present SchClass(FreeSchema) begin
        Name::AttrType
        Class::Ob
        subject::Attr(Class, Name)
    end
    @acset_type Class(SchClass)
    classes = Class{Symbol}()
    
    class_db = DBSource(SQLite.DB(), acset_schema(classes))

    using FunSQL
    execute![τ](class_db, FunSQL.render(class_db, classes))
    # execute!(class_db, "create table `Class` (_id int, subject varchar(255))")

    class_src = add_source!(fabric, class_db)

    reflect!(fabric)

end

@testset "Many Columns" begin

    fabric = DataFabric()

    @present SchRainbow(FreeSchema) begin
        (Red, Orange, Yellow, Green, Blue, Indigo, Violet)::AttrType
        Hue::Ob
        red::Attr(Hue, Red)
        orange::Attr(Hue, Orange)
        yellow::Attr(Hue, Yellow)
        green::Attr(Hue, Green)
        blue::Attr(Hue, Blue)
        indigo::Attr(Hue, Indigo)
        violet::Attr(Hue, Violet)
    end
    @acset_type Rainbow(SchRainbow)
    rainbow = Rainbow{Int, Symbol, DataType, Vector, Char, Real, Bool}()
    rainbow_db = DBSource(SQLite.DB(), acset_schema(rainbow))

    using FunSQL
    execute![τ](rainbow_db, FunSQL.render(rainbow_db, rainbow)) 

    rainbow_src = add_source!(fabric, rainbow_db)

    reflect!(fabric)

end
