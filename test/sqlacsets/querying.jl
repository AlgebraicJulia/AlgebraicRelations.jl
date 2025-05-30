using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

using Test

include("examples/wineries.jl");

diag = @relation (winemaker_name=winemaker_name) begin
    WineWinemaker(wwm_wine=wine, wwm_winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker_name)
    CountryClimate(id=region, cc_country=country_id)
    Country(id=country_id, country=country)
    Wine(id=wine, cultivar=grape)
    Grape(id=grape, color=color, species=species)
end

df_green = query(fabric, diag, (species=:GreenGrape, color=:Green, country=:Italy))

@test df_green.winemaker == ["Planeta", "Odoardi", "GianfrancoFino", "Ornellaia", "MicheleChiarlo"]

df_red = query(fabric, diag, (species=:RedGrape, color=:Red, country=:Italy))

@test df_red.winemaker == ["Donnafugata", "Florio", "Librandi", "Tormaresca", "Antinori", "LeoneDeCastris", "Gaja"]

# TODO need to throw error when params are not given
@test_throws Exception query(fabric, diag, (country=:USA,)) #= this breaks =#
