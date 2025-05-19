using Test

using ACSets
using Catlab
using AlgebraicRelations

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
