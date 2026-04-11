@present SchWineyard3(FreeSchema) begin
    (Climate, Grape, ClimateGrape, Cheese, GrapeCheese, CheeseClimate)::Ob
    cg_climate::Hom(ClimateGrape, Climate)
    cg_grape::Hom(ClimateGrape, Grape)
    gc_grape::Hom(GrapeCheese, Grape)
    gc_cheese::Hom(GrapeCheese, Cheese)
    cc_cheese::Hom(CheeseClimate, Cheese)
    cc_climate::Hom(CheeseClimate, Climate)
    #
    Desc::AttrType
    name::Attr(Grape, Desc)
    desc::Attr(Climate, Desc)
    cheese_name::Attr(Cheese, Desc)
end
@acset_type Wineyard3(SchWineyard3)

w3 = Wineyard3{String}()

add_part!(w3, :Climate, desc="Hot, Dry")
add_part!(w3, :Climate, desc="Mild")
add_part!(w3, :Climate, desc="Cool, Wet")

add_part!(w3, :Grape, name="Green")
add_part!(w3, :Grape, name="Red")
add_part!(w3, :Grape, name="Black")

add_part!(w3, :Cheese, cheese_name="Brie")
add_part!(w3, :Cheese, cheese_name="Cheddar")
add_part!(w3, :Cheese, cheese_name="Gouda")

add_part!(w3, :ClimateGrape, cg_climate=1, cg_grape=1)
add_part!(w3, :ClimateGrape, cg_climate=1, cg_grape=2)
add_part!(w3, :ClimateGrape, cg_climate=2, cg_grape=2)
add_part!(w3, :ClimateGrape, cg_climate=3, cg_grape=3)

add_part!(w3, :GrapeCheese, gc_grape=1, gc_cheese=1)
add_part!(w3, :GrapeCheese, gc_grape=2, gc_cheese=2)
add_part!(w3, :GrapeCheese, gc_grape=2, gc_cheese=3)
add_part!(w3, :GrapeCheese, gc_grape=3, gc_cheese=1)

add_part!(w3, :CheeseClimate, cc_cheese=2, cc_climate=1)
add_part!(w3, :CheeseClimate, cc_cheese=3, cc_climate=2)
add_part!(w3, :CheeseClimate, cc_cheese=1, cc_climate=3)

data3 = encode_attr(w3)

# "For each grape+cheese+climate triple where the grape grows in
#  that climate and the cheese pairs with both the grape and
#  that climate, return the grape name, cheese name, and climate description."

d3 = @relation (gn=gn, cn=cn, cd=cd) begin
    ClimateGrape(cg_grape=gr, cg_climate=cl)
    GrapeCheese(gc_grape=gr, gc_cheese=ch)
    CheeseClimate(cc_cheese=ch, cc_climate=cl)
    Grape(id=gr, name=gn)
    Cheese(id=ch, cheese_name=cn)
    Climate(id=cl, desc=cd)
end

result = prepare(d3, w3, data3)
# Expected (3 output rows × 3 columns):
#   gn: [2, 2, 3]        Red,   Red,   Black
#   cn: [2, 3, 1]        Cheddar, Gouda, Brie
#   cd: [1, 2, 3]        Hot,   Mild,  Cool
