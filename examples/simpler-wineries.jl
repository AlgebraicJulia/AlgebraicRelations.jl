using ACSets
using Catlab
using AlgebraicRelations

using SQLite, DBInterface
using FunSQL

τ = AlgebraicRelations.SQL.DatabaseDS.DBSourceTrait()
fabric = DataFabric()

@present SchClimate(FreeSchema) begin
    Name::AttrType
    Climate::Ob
    climate_type::Attr(Climate, Name)
end
@acset_type Climate(SchClimate)
climate = InMemory(Climate{Symbol}())
climate_src = add_source!(fabric, climate)
# data
add_part!(fabric, :Climate, climate_type=:Cool)
add_part!(fabric, :Climate, climate_type=:Intermediate)
add_part!(fabric, :Climate, climate_type=:Warm)
add_part!(fabric, :Climate, climate_type=:Hot)

@present SchGrape(FreeSchema) begin
    (Name, Climate)::AttrType
    Grape::Ob
    (color, species)::Attr(Grape, Name)
end
@acset_type Grape(SchGrape)
grape = InMemory(Grape{Symbol, FK{Climate}}())
grape_src = add_source!(fabric, grape)
#
add_part!(fabric, :Grape, color=:Green, species=:GreenGrape)
add_part!(fabric, :Grape, color=:Red, species=:RedGrape)

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
# data
add_part!(fabric, :Country, country=:Italy)
add_part!(fabric, :Country, country=:France)
add_part!(fabric, :Country, country=:USA)
add_part!(fabric, :Country, country=:NewZealand)

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
# data
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(1), cc_climate=FK{Climate}(2), cc_region=:Sicily)
# chardonnay, nero d'avola, marsalacc_
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(1), cc_climate=FK{Climate}(2), cc_region=:Calabria)
# gaglioppo, greco bianco
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(1), cc_climate=FK{Climate}(2), cc_region=:Puglia)
# sangiovese, montepulciano, trebbicc_ano
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(1), cc_climate=FK{Climate}(2), cc_region=:Tuscany)
# sangiovese, merlot, trebbiano
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(1), cc_climate=FK{Climate}(2), cc_region=:Piedmont)
# nebbiolo, moscato d'asti
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(3), cc_climate=FK{Climate}(3), cc_region=:NapaValley)
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(3), cc_climate=FK{Climate}(1), cc_region=:WillametteValley)
add_part!(fabric, :CountryClimate, cc_country=FK{Country}(3), cc_climate=FK{Climate}(2), cc_region=:Sonoma)

_d = @relation (country=co, region=region) begin
    CountryClimate(cc_country=co, cc_region=region)
    Country(id=co)
end

_d2 = @relation (country_name=cn, region=region) begin
    CountryClimate(cc_country=co, cc_region=region)
    Country(id=co, country=cn)
end
