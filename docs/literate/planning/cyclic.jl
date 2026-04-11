# Query: "Find grapes that grow in a climate AND pair with a cheese 
#          that is also associated with that same climate"
@present SchWineyard2(FreeSchema) begin
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
@acset_type Wineyard2(SchWineyard2)

w2 = Wineyard2{String}()

add_part!(w2, :Climate, desc="Hot, Dry")
add_part!(w2, :Climate, desc="Mild")
add_part!(w2, :Climate, desc="Cool, Wet")
#
add_part!(w2, :Grape, name="Green")
add_part!(w2, :Grape, name="Red")
add_part!(w2, :Grape, name="Black")
#
add_part!(w2, :Cheese, cheese_name="Brie")
add_part!(w2, :Cheese, cheese_name="Cheddar")
add_part!(w2, :Cheese, cheese_name="Gouda")
#
add_part!(w2, :ClimateGrape, cg_climate=1, cg_grape=1)
add_part!(w2, :ClimateGrape, cg_climate=1, cg_grape=2)
add_part!(w2, :ClimateGrape, cg_climate=2, cg_grape=2)
add_part!(w2, :ClimateGrape, cg_climate=3, cg_grape=3)
#
add_part!(w2, :GrapeCheese, gc_grape=1, gc_cheese=1)
add_part!(w2, :GrapeCheese, gc_grape=2, gc_cheese=2)
add_part!(w2, :GrapeCheese, gc_grape=2, gc_cheese=3)
add_part!(w2, :GrapeCheese, gc_grape=3, gc_cheese=1)
#
add_part!(w2, :CheeseClimate, cc_cheese=2, cc_climate=1)  # Cheddar + Hot
add_part!(w2, :CheeseClimate, cc_cheese=3, cc_climate=2)  # Gouda + Mild
add_part!(w2, :CheeseClimate, cc_cheese=1, cc_climate=3)  # Brie + Cool

data = encode_attr(w2)

_d = @relation (gn=gn) begin
    ClimateGrape(cg_grape=gr, cg_climate=cl)
    GrapeCheese(gc_grape=gr, gc_cheese=ch)
    CheeseClimate(cc_cheese=ch, cc_climate=cl)
    Grape(id=gr, name=gn)
end
outputs = subpart(_d, :outer_junction)

# climategrape = vcat(subpart(w2, :cg_grape)', subpart(w2, :cg_climate)')
# grapecheese = vcat(subpart(w2, :gc_grape)', subpart(w2, :gc_cheese)')
# cheeseclimate = vcat(subpart(w2, :cc_cheese)', subpart(w2, :cc_climate)')
# # climate = [2;;]  # select Mild
# grape = vcat(parts(w2, :Grape)', data[:Grape][:name].encoded')
# # cheese = vcat(parts(w, :Cheese)', data[:Cheese][:cheese_name].encoded')


# data[:Grape][:name][[result...]]

