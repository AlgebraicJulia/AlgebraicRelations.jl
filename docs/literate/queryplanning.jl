using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

include("examples/wineries.jl");

incident(fabric, :GreenGrape, :species)

incident(fabric, [(:Green, :color), (:GreenGrape, :species)])

diag = @relation (winemaker_name=winemaker_name) begin
    WineWinemaker(wwm_wine=wine, wwm_winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker_name)
    CountryClimate(id=region, cc_country=country_id)
    Country(id=country_id, country=country)
    Wine(id=wine, cultivar=grape)
    Grape(id=grape, color=color, species=species)
end

query(fabric, diag, (species=:GreenGrape, color=:Green, country=:Italy))

query(fabric, diag, (species=:RedGrape, color=:Red, country=:Italy))

# TODO need to throw error when params are not given
query(fabric, diag, (country=:USA,)) #= this breaks =#

view_graphviz(to_graphviz(diag, box_labels=:name, junction_labels=:variable))

view_graphviz(to_graphviz(diag, box_labels=true))

# look at the UWD in question
# view_graphviz(to_graphviz(get_port, box_labels=true, junction_labels=:variable))

# helpful to have the Schema visualized when doing lots of subpart/incident
view_graphviz(to_graphviz(Presentation(acset_schema(diag)), graph_attrs=Dict(:size=>"5", :ratio=>"expand")))
