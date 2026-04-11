using ACSets
using Catlab
using AlgebraicRelations

using FunSQL

fabric = DataFabric()

@present SchClimate(FreeSchema) begin
    Name::AttrType
    Climate::Ob
    climate_type::Attr(Climate, Name)
end
@acset_type Climate(SchClimate)
climate = InMemory(Climate{Symbol}())
climate_src = add_source!(fabric, climate)

@present SchGrape(FreeSchema) begin
    (Name, Climate)::AttrType
    Grape::Ob
    (color, species)::Attr(Grape, Name)
end
@acset_type Grape(SchGrape)
grape = InMemory(Grape{Symbol, FK{Climate}}())
grape_src = add_source!(fabric, grape)

@present SchClimateGrape(FreeSchema) begin
    (Grape, Climate)::AttrType
    ClimateGrape::Ob
    cg_grape::Attr(ClimateGrape, Grape)
    cg_climate::Attr(ClimateGrape, Climate)
end
@acset_type ClimateGrape(SchClimateGrape)
climate_grape = InMemory(ClimateGrape{FK{Grape}, FK{Climate}}())
climate_grape_src = add_source!(fabric, climate_grape)
add_fk!(fabric, climate_grape_src, grape_src, :ClimateGrape!cg_grape => :Grape!Grape_id)
add_fk!(fabric, climate_grape_src, climate_src, :ClimateGrape!cg_climate => :Climate!Climate_id)

@present SchCountry(FreeSchema) begin
    Name::AttrType
    Country::Ob
    country::Attr(Country, Name)
end
@acset_type Country(SchCountry)
country = InMemory(Country{Symbol}())
country_src = add_source!(fabric, country)

@present SchCountryClimate(FreeSchema) begin
    (Name, Country, Climate)::AttrType
    CountryClimate::Ob
    cc_country::Attr(CountryClimate, Country)
    cc_climate::Attr(CountryClimate, Climate)
    cc_region::Attr(CountryClimate, Name)
end
@acset_type CountryClimate(SchCountryClimate)
country_climate = InMemory(CountryClimate{Symbol, FK{Country}, FK{Climate}}())
country_climate_src = add_source!(fabric, country_climate)
add_fk!(fabric, country_climate_src, country_src, :CountryClimate!cc_country => :Country!Country_id)
add_fk!(fabric, country_climate_src, climate_src, :CountryClimate!cc_climate => :Climate!Climate_id)

ingest_csv!(fabric, :Climate, "examples/simpler-wineries/Climate.csv")
ingest_csv!(fabric, :Grape, "examples/simpler-wineries/Grape.csv")
ingest_csv!(fabric, :Country, "examples/simpler-wineries/Country.csv")

# junction tables — specify which columns are FKs
ingest_csv!(fabric, :ClimateGrape, "examples/simpler-wineries/ClimateGrape.csv",
    fk_types=Dict(:cg_grape => Grape, :cg_climate => Climate))

ingest_csv!(fabric, :CountryClimate, "examples/simpler-wineries/CountryClimate.csv",
    fk_types=Dict(:cc_country => Country, :cc_climate => Climate))

_d = @relation (country=co, region=region) begin
    CountryClimate(cc_country=co, cc_region=region)
    Country(id=co)
end

_d2 = @relation (country_name=cn, region=region) begin
    CountryClimate(cc_country=co, cc_region=region)
    Country(id=co, country=cn)
end
