using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
# using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

include("examples/wineries.jl");

fabric

catalog(fabric)

view_graphviz(fabric.graph) 

_d = @relation (nation_id=nation_id) begin
   WineryNation(winery_id, nation_id)
   WineryGrape(winery_id, grape_id)
   Grape(grape_id)
end

diag = @relation (winemaker_name=winemaker_name) begin
    WineWinemaker(wwm_wine=wine, wwm_winemaker=winemaker_id)
    Winemaker(id=winemaker_id, region=region, winemaker=winemaker_name)
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

# ====
using WiringDiagrams


# grab the output
output = diag[:outer_junction]

dims = Dict(box => 1 for box in boxes(diag)) 

d = WiringDiagram(schemas, output, dims)


# =======================
using WiringDiagrams

@present SchWineyard(FreeSchema) begin
    (Climate, Grape, ClimateGrape)::Ob
    climate::Hom(ClimateGrape, Climate)
    grape::Hom(ClimateGrape, Grape)
    #
    Desc::AttrType
    name::Attr(Grape, Desc)
    desc::Attr(Climate, Desc)
end
@acset_type Wineyard(SchWineyard)

w = Wineyard{String}()
add_part!(w, :Climate, desc="Hot, Dry")
add_part!(w, :Climate, desc="Mild")
add_part!(w, :Grape, name="Green")
add_part!(w, :Grape, name="Red")
add_part!(w, :ClimateGrape, climate=1, grape=1)
add_part!(w, :ClimateGrape, climate=1, grape=2)
add_part!(w, :ClimateGrape, climate=2, grape=2)

_d = @relation (name=name) begin
    ClimateGrape(grape, climate)
    Climate(climate)
    Grape(grape, name)
end
_s = query_inputs(_d)

outputs = subpart(_d, :outer_junction)

data = encode_attr(w)

climategrape = vcat(subpart(w, :grape)', subpart(w, :climate)')
climate = [1;;]
# since name is not an integer value, it must be replaced by an encoding
grape = vcat(parts(w, :Grape)', data[:Grape][:name].encoded')

dims = Dict(1 => 2, 2 => 2, 3 => 2)  # wire 1=grape(2), wire 2=climate(2), wire 3=name(2)

d = WiringDiagrams.WiringDiagram(_s, outputs, dims) 
a = SpanAlgebra{Matrix{Int}}()

result = a(d)([climategrape, climate, grape]...)
# returns "2" which corresponds to the name of the grape which grows in hot (==2) climates


# ==================================
@present SchWineData(FreeSchema) begin
    (Climate, Grape, ClimateGrape, Cheese, GrapeCheese)::Ob
    climate::Hom(ClimateGrape, Climate)
    grape::Hom(ClimateGrape, Grape)
    cheese_grape::Hom(GrapeCheese, Grape)
    cheese::Hom(GrapeCheese, Cheese)
    #
    Desc::AttrType
    name::Attr(Grape, Desc)
    desc::Attr(Climate, Desc)
    cheese_name::Attr(Cheese, Desc)
end
@acset_type WineInfo(SchWineData)

w = WineInfo{String}()

# Climates
add_part!(w, :Climate, desc="Hot, Dry")     # 1
add_part!(w, :Climate, desc="Mild")          # 2
add_part!(w, :Climate, desc="Cool, Wet")     # 3
# Grapes
add_part!(w, :Grape, name="Green")           # 1
add_part!(w, :Grape, name="Red")             # 2
add_part!(w, :Grape, name="Black")           # 3
# ClimateGrape associations
add_part!(w, :ClimateGrape, climate=1, grape=1)  # Hot → Green
add_part!(w, :ClimateGrape, climate=1, grape=2)  # Hot → Red
add_part!(w, :ClimateGrape, climate=2, grape=2)  # Mild → Red
add_part!(w, :ClimateGrape, climate=3, grape=3)  # Cool → Black
# Cheeses
add_part!(w, :Cheese, cheese_name="Brie")        # 1
add_part!(w, :Cheese, cheese_name="Cheddar")     # 2
add_part!(w, :Cheese, cheese_name="Gouda")       # 3
# GrapeCheese pairings
add_part!(w, :GrapeCheese, cheese_grape=1, cheese=1)  # Green + Brie
add_part!(w, :GrapeCheese, cheese_grape=2, cheese=2)  # Red + Cheddar
add_part!(w, :GrapeCheese, cheese_grape=2, cheese=3)  # Red + Gouda
add_part!(w, :GrapeCheese, cheese_grape=3, cheese=1)  # Black + Brie

_d = @relation (cn=cn) begin
    ClimateGrape(g, cl)
    Climate(cl)
    Grape(g, gn)
    GrapeCheese(g, gc)
    Cheese(gc, cn)
end

outputs = subpart(_d, :outer_junction)

data = encode_attr(w)

climategrape = vcat(subpart(w, :grape)', subpart(w, :climate)')
climate = [2;;]  # select Mild
grape = vcat(parts(w, :Grape)', data[:Grape][:name].encoded')
grapecheese = vcat(subpart(w, :cheese_grape)', subpart(w, :cheese)')
cheese = vcat(parts(w, :Cheese)', data[:Cheese][:cheese_name].encoded')

_s = query_inputs(_d)
outputs = subpart(_d, :outer_junction)
dims = Dict(w => 3 for w in 1:maximum(maximum.(_s)))
d = WiringDiagrams.WiringDiagram(_s, outputs, dims)

d = WiringDiagrams.WiringDiagram(_s, outputs, dims) 
a = SpanAlgebra{Matrix{Int}}()

result = a(d)([climategrape, climate, grape, grapecheese, cheese]...)

data[:Cheese][:name](result)

# ==================================

schemas = [
    [1, 2],  # climate hom: climate_grape → climate
    [1, 3],  # grape hom: climate_grape → grape
    [2],     # climate filter: select climate 2 (Mild)
]

output = [3]  # project onto grape

dims = Dict(1 => 3, 2 => 2, 3 => 2)

d = WiringDiagrams.WiringDiagram(schemas, output, dims)
a = SpanAlgebra{Matrix{Int}}()

# The climate hom column: subpart(w, :climate) = [1, 1, 2]
climate_hom = [
    # 1 2 3;  # climate_grape row id
    1 1 2   # climate id
]

# The grape hom column: subpart(w, :grape) = [1, 2, 2]
grape_hom = [
    # 1 2 3;  # climate_grape row id
    1 2 2   # grape id
]

# Filter: select climate 2 (Mild)
climate_filter = [2;;]

result = a(d)(climate_hom, grape_hom, climate_filter)
# Should return [2] — grape 2 (Red), the only grape in mild climate

# Wires (junctions):
#   1 = wine
#   2 = winery
#   3 = country
#   4 = grape
#   5 = color

# Boxes (tables/relations):
#   1: wine_winery (wine, winery)
#   2: winery_country (winery, country)
#   3: wine_grape (wine, grape)
#   4: grape_color (grape, color)
#   5: color_filter (color)  — selects "red"

schemas = [
    [1, 2],  # wine_winery
    [2, 3],  # winery_country
    [1, 4],  # wine_grape
    [4, 5],  # grape_color
    [5],     # color_filter
]

output = [3]  # project onto country

dims = Dict(1 => 4, 2 => 3, 3 => 3, 4 => 3, 5 => 2)

d = WiringDiagram.WiringDiagram(schemas, output, dims)
a = SpanAlgebra{Matrix{Int}}()

# Data (columns are records):

wine_winery = [
    1 2 3 4;   # wine
    1 1 2 3    # winery
]

winery_country = [
    1 2 3;     # winery
    1 2 2      # country (1=France, 2=Italy, 3=Spain)
]

wine_grape = [
    1 2 3 4;   # wine
    1 1 2 3    # grape
]

grape_color = [
    1 2 3;     # grape
    1 1 2      # color (1=red, 2=white)
]

color_filter = [1;;]  # select color 1 (red)

result = a(d)(wine_winery, winery_country, wine_grape, grape_color, color_filter)
# result should be [1; 2;; ...] — France and Italy
    
