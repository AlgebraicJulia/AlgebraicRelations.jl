using ACSets
using Catlab
using AlgebraicRelations

# impl Description
# impl Auditing

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

@present SchInfoSource(FreeSchema) begin
    (Name, Description)::AttrType
    InfoSource::Ob
    code::Attr(InfoSource, Name)
    desc::Attr(InfoSource, Description)
end
@acset_type InfoSource(SchInfoSource)
infosources = InMemory(InfoSource{Symbol, String}())
infosource_src = add_source!(fabric, infosources)

@present SchRatingGuide(FreeSchema) begin
    Name::AttrType
    RatingGuide::Ob
    (color, desc)::Attr(RatingGuide, Name)
end
@acset_type RatingGuide(SchRatingGuide)
ratingguide = InMemory(RatingGuide{Symbol}())
add_source!(fabric, ratingguide)

@present SchWineColor(FreeSchema) begin
    Name::AttrType
    WineColor::Ob
    color::Attr(WineColor, Name)
end
@acset_type WineColor(SchWineColor)
winecolor = InMemory(WineColor{Symbol}())
winecolor_src = add_source!(fabric, winecolor)

@present SchWine(FreeSchema) begin
    (Name, Price, WineColor, Winemaker)::AttrType
    Wine::Ob
    color::Attr(Wine, WineColor)
    maker::Attr(Wine, Winemaker)
    # TODO winemaker?
    (code, name, desc, good_years)::Attr(Wine, Name)
    (bottle_price, half_price)::Attr(Wine, Price)
end
@acset_type Wine(SchWine)
wine = InMemory(Wine{Symbol, Int, FK{WineColor}, FK{Winemaker}}())
wine_src = add_source!(fabric, wine)
add_fk!(fabric, wine_src, winecolor_src, :Wine!color => :WineColor!WineColor_id)
add_fk!(fabric, wine_src, winecolor_src, :Wine!color => :WineColor!WineColor_id)

@present SchFood(FreeSchema) begin
    Name::AttrType
    Food::Ob
    comments::Attr(Food, Name)
    # two primary keys
end
@acset_type Food(SchFood)
food = InMemory(Food{Symbol}())
food_src = add_source!(fabric, food)

# junction
@present SchWineFood(FreeSchema) begin
    (Wine, Food)::AttrType
    WineFood::Ob
    food::Attr(WineFood, Food)
    wine::Attr(WineFood, Wine)
end
@acset_type WineFood(SchWineFood)
winefood = InMemory(WineFood{FK{Wine}, FK{Food}}())
winefood_src = add_source!(fabric, winefood)
add_fk!(fabric, winefood_src, food_src, :WineFood!food => :Food!Food_id)
add_fk!(fabric, winefood_src, winemaker_src, :WineFood!wine => :Winemaker!Winemaker_id)

@present SchMerchant(FreeSchema) begin
    Name::AttrType
    Merchant::Ob
    name::Attr(Merchant, Name)
end
@acset_type Merchant(SchMerchant)
merchants = InMemory(Merchant{Symbol}())
merchant_src = add_source!(fabric, merchants)

@present SchWineMerchant(FreeSchema) begin
    (Price, DateInterval, Wine, Merchant)::AttrType
    WineMerchant::Ob
    wine::Attr(WineMerchant, Wine)
    merchant::Attr(WineMerchant, Merchant)
    interval::Attr(WineMerchant, DateInterval)
    price::Attr(WineMerchant, Price)
end
@acset_type WineMerchant(SchWineMerchant)
winemerchants = InMemory(WineMerchant{Int, Symbol, FK{Wine}, FK{Merchant}}())
winemerchant_src = add_source!(fabric, winemerchants)
add_fk!(fabric, winemerchant_src, wine_src, :WineMerchant!wine => :Wine!Wine_id)
add_fk!(fabric, winemerchant_src, merchant_src, :WineMerchant!merchant => :Merchant!Merchant_id)

# TODO all columns have the INTEGER type
reflect!(fabric)

# won't work until reflection happens
add_part!(fabric, :Country, country=:Antarctica, code=:ANT) 

subpart(fabric, :name) 
# TODO use accessor (!) syntax here, since there are multiple columns called `name`

# code needs to be an integer referencing country code
add_part!(fabric, :Winemaker, wm_name=:BJs, country_code=FK{Country}(1))

subpart(fabric, :country_code)

subpart(fabric, :Winemaker => :wm_name)

add_part!(fabric, :InfoSource, code=:something, desc="a nice description")

subpart(fabric, :desc)

# TODO is Color a FK?
add_part!(fabric, :RatingGuide, color=:red, desc=:description)

add_part!(fabric, :WineColor, color=:red)

add_part!(fabric, :Wine, color=FK{WineColor}(1), maker=FK{Winemaker}(1), code=:chianti, name=:Chianti, desc=:dry, good_years=:should_be_int, bottle_price=6, half_price=3)

add_part!(fabric, :Food, comments=:should_be_string)

add_part!(fabric, :WineFood, food=FK{Food}(1), wine=FK{Wine}(1))

add_part!(fabric, :Merchant, name=:ABC)

add_part!(fabric, :WineMerchant, wine=FK{Wine}(1), merchant=FK{Merchant}(1), interval=:week, price=1)

subpart(fabric, [:maker, :country_code, :country])
