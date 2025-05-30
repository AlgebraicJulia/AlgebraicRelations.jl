using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface

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

# TODO add data

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

@present SchWine(FreeSchema) begin
    (Name, Price, Grape)::AttrType
    Wine::Ob
    cultivar::Attr(Wine, Grape)
    (type, name)::Attr(Wine, Name)
end
@acset_type Wine(SchWine)
wine = InMemory(Wine{Symbol, Int, FK{Grape}}())
wine_src = add_source!(fabric, wine)
add_fk!(fabric, wine_src, grape_src, :Wine!cultivar => :Grape!Grape_id)

# data
add_part!(fabric, :Wine, type=:white, cultivar=FK{Grape}(1), name=:Chardonnay) # Sicily
add_part!(fabric, :Wine, type=:red, cultivar=FK{Grape}(2), name=:NeroDAvola) # Sicily
add_part!(fabric, :Wine, type=:fortified, cultivar=FK{Grape}(2), name=:Marsala) # Sicily
add_part!(fabric, :Wine, type=:red, cultivar=FK{Grape}(2), name=:Gaglioppo) # Calabria
add_part!(fabric, :Wine, type=:white, cultivar=FK{Grape}(1), name=:GrecoBianco) # Calabria
add_part!(fabric, :Wine, type=:red, cultivar=FK{Grape}(2), name=:Sangiovese) # Puglia and Tuscany
add_part!(fabric, :Wine, type=:red, cultivar=FK{Grape}(2), name=:Montepulciano) # Puglia
add_part!(fabric, :Wine, type=:white, cultivar=FK{Grape}(1), name=:Trebbiano) # Puglia and Tuscany
add_part!(fabric, :Wine, type=:red, cultivar=FK{Grape}(2), name=:Nebbiolo) # Piedmont
add_part!(fabric, :Wine, type=:white, cultivar=FK{Grape}(1), name=:MoscatoDAsti) # Piedmont

@present SchWinemaker(FreeSchema) begin
    (Name, Region)::AttrType
    Winemaker::Ob
    region::Attr(Winemaker, Region)
    winemaker::Attr(Winemaker, Name) # TODO "name" does not get entered
    # fk constraint means that there is *some* schema out there
end
@acset_type Winemaker(SchWinemaker)
winemakers = Winemaker{Symbol, FK{CountryClimate}}()
winemaker_db = DBSource(SQLite.DB(), acset_schema(winemakers))

# load table
import FunSQL: render
execute!(winemaker_db, render(winemaker_db, winemakers))
# add to fabric
winemaker_src = add_source!(fabric, winemaker_db, :Winemaker)
add_fk!(fabric, winemaker_src, country_climate_src, :Winemaker!region => :CountryClimate!CountryClimate_id)
# TODO when the column is incorrect, need more helpful error

# data
add_part!(fabric, :Winemaker, [
    (_id=1, region=FK{CountryClimate}(1), winemaker=:Planeta), #char
    (_id=2, region=FK{CountryClimate}(1), winemaker=:Donnafugata), #nero d'avola
    (_id=3, region=FK{CountryClimate}(1), winemaker=:Florio), # marsala
    (_id=4, region=FK{CountryClimate}(2), winemaker=:Librandi), # gaglioppo
    (_id=5, region=FK{CountryClimate}(2), winemaker=:Odoardi), # greco bianco
    (_id=6, region=FK{CountryClimate}(3), winemaker=:Tormaresca),
    (_id=7, region=FK{CountryClimate}(4), winemaker=:Antinori),
    (_id=8, region=FK{CountryClimate}(3), winemaker=:LeoneDeCastris),
    (_id=9, region=FK{CountryClimate}(3), winemaker=:GianfrancoFino),
    (_id=10,region=FK{CountryClimate}(4), winemaker=:Ornellaia),
    (_id=11,region=FK{CountryClimate}(5), winemaker=:Gaja),
    (_id=12,region=FK{CountryClimate}(5), winemaker=:MicheleChiarlo)])
# TODO better if auto-increment

@present SchWineWinemaker(FreeSchema) begin
    (Name, Wine, Winemaker)::AttrType
    WineWinemaker::Ob
    wwm_wine::Attr(WineWinemaker, Wine)
    wwm_winemaker::Attr(WineWinemaker, Winemaker)
end
@acset_type WineWinemaker(SchWineWinemaker)
wine_winemaker = InMemory(WineWinemaker{Symbol, FK{Wine}, FK{Winemaker}}())
wine_winemaker_src = add_source!(fabric, wine_winemaker)
add_fk!(fabric, wine_winemaker_src, wine_src, :WineWinemaker!wwm_wine => :Wine!Wine_id)
add_fk!(fabric, wine_winemaker_src, winemaker_src, :WineWinemaker!wwm_winemaker => :Winemaker!Winemaker_id)

add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(1), wwm_winemaker=FK{Winemaker}(1))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(2), wwm_winemaker=FK{Winemaker}(2))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(3), wwm_winemaker=FK{Winemaker}(3))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(4), wwm_winemaker=FK{Winemaker}(4))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(5), wwm_winemaker=FK{Winemaker}(5))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(6), wwm_winemaker=FK{Winemaker}(6))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(6), wwm_winemaker=FK{Winemaker}(7))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(7), wwm_winemaker=FK{Winemaker}(8))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(8), wwm_winemaker=FK{Winemaker}(9))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(8), wwm_winemaker=FK{Winemaker}(10))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(9), wwm_winemaker=FK{Winemaker}(11))
add_part!(fabric, :WineWinemaker, wwm_wine=FK{Wine}(10), wwm_winemaker=FK{Winemaker}(12))

@present SchFood(FreeSchema) begin
    Name::AttrType
    Food::Ob
    food::Attr(Food, Name)
    comments::Attr(Food, Name)
    # two primary keys
end
@acset_type Food(SchFood)
food = InMemory(Food{String}())
food_src = add_source!(fabric, food)

# data
add_part!(fabric, :Food, food="cheese", comments="sharp")

# junction
@present SchWineFood(FreeSchema) begin
    (Wine, Food)::AttrType
    WineFood::Ob
    wf_food::Attr(WineFood, Food)
    wf_wine::Attr(WineFood, Wine)
end
@acset_type WineFood(SchWineFood)
winefood = InMemory(WineFood{FK{Wine}, FK{Food}}())
winefood_src = add_source!(fabric, winefood)
add_fk!(fabric, winefood_src, food_src, :WineFood!wf_food => :Food!Food_id)
add_fk!(fabric, winefood_src, winemaker_src, :WineFood!wf_wine => :Wine!Wine_id)

# data
add_part!(fabric, :WineFood, wf_food=FK{Food}(1), wf_wine=FK{Wine}(1))
# TODO does not guard against constraint

# TODO need to specify where 
# execute!(winemaker_db, "select * from Winemaker")

# fabric
