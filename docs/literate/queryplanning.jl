using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
# using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

include("examples/wineries.jl");

fabric

catalog(fabric)

view_graphviz(fabric.graph) 

diag = @relation (winemaker_name=winemaker_name) begin
    WineWinemaker(wwm_wine=wine, wwm_winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker_name) # SQLite
    CountryClimate(id=region, cc_country=country_id)
    Country(id=country_id, country=country)
    Wine(id=wine, cultivar=grape)
    Grape(id=grape, color=color, species=species)
end

view_graphviz(to_graphviz(diag, box_labels=:name, junction_labels=false))

view_graphviz(to_graphviz(diag, box_labels=true))

query(fabric, diag, (species=:GreenGrape, color=:Green, country=:Italy))

query(fabric, diag, (species=:GreenGrape, color=:Green, country=:USA))

query(fabric, diag, (species=:RedGrape, color=:Red, country=:USA))
