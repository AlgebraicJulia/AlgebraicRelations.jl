using ACSets
using Catlab, Catlab.Graphics.Graphviz
using AlgebraicRelations
using SQLite, DBInterface

fabric = DataFabric()

@present SchCountry(FreeSchema) begin
    Name::AttrType
    Country::Ob
    (country, code)::Attr(Country, Name)
end
@acset_type Country(SchCountry)
country = InMemory(Country{Symbol}())
country_src = add_source!(fabric, country)

# data
add_part!(fabric, :Country, country=:Italy, code=:IT)
add_part!(fabric, :Country, country=:France, code=:FR)
add_part!(fabric, :Country, country=:USA, code=:US)

@present SchWinemaker(FreeSchema) begin
    (Name, Country)::AttrType
    Winemaker::Ob
    country_code::Attr(Winemaker, Country)
    winemaker::Attr(Winemaker, Name) # TODO "name" does not get entered
    # fk constraint means that there is *some* schema out there
end
@acset_type Winemaker(SchWinemaker)
winemakers = Winemaker{Symbol, FK{Country}}()
winemaker_db = DBSource(SQLite.DB(), acset_schema(winemakers))

# load table
import FunSQL: render
execute!(winemaker_db, render(winemaker_db, winemakers))

winemaker_src = add_source!(fabric, winemaker_db, :Winemaker)
add_fk!(fabric, winemaker_src, country_src, :Winemaker!country_code => :Country!Country_id)

# data
add_part!(fabric, :Winemaker, 
    [(_id=1, country_code=1, winemaker=:Banfi)
    ,(_id=2, country_code=2, winemaker=:NotBanfi)
    ,(_id=3, country_code=2, winemaker=:BanfiAsWell)
    ,(_id=4, country_code=3, winemaker=:AmericanBanfi)])

@present SchWine(FreeSchema) begin
    (Name, Price, Winemaker)::AttrType
    Wine::Ob
    maker::Attr(Wine, Winemaker)
    color::Attr(Wine, Name)
    # TODO winemaker?
    (code, name, desc, good_years)::Attr(Wine, Name)
    (bottle_price, half_price)::Attr(Wine, Price)
end
@acset_type Wine(SchWine)
wine = InMemory(Wine{Symbol, Int, FK{Winemaker}}())
wine_src = add_source!(fabric, wine)
add_fk!(fabric, wine_src, winemaker_src, :Wine!maker => :Winemaker!Winemaker_id)

# data
add_part!(fabric, :Wine, color=:red, maker=FK{Winemaker}(1), code=:chianti, name=:Chianti, desc=:dry, good_years=:should_be_int, bottle_price=6, half_price=3)

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
    food::Attr(WineFood, Food)
    wine::Attr(WineFood, Wine)
end
@acset_type WineFood(SchWineFood)
winefood = InMemory(WineFood{FK{Wine}, FK{Food}}())
winefood_src = add_source!(fabric, winefood)
add_fk!(fabric, winefood_src, food_src, :WineFood!food => :Food!Food_id)
add_fk!(fabric, winefood_src, winemaker_src, :WineFood!wine => :Winemaker!Winemaker_id)

# data
add_part!(fabric, :WineFood, food=FK{Food}(1), wine=FK{Wine}(1))


view_graphviz(to_graphviz(fabric.graph))

# TODO does not guard against constraint

execute!(winemaker_db, "select * from Winemaker")

using MLStyle: @match, @λ
using ACSets.Query
import ACSets.Query: WhereCondition, AndWhere, OrWhere

# TODO add the Join statement to track the first where
q = From(:Winemaker) |> Where([:Winemaker, :country_code], ==, [:Country, :id]) |>
                        Where([:Country, :country], ==, "France") |>
                        Select(:Country!country)

struct TableConds
    conds::Dict{Vector{Symbol}, Vector{WhereCondition}}
end

function TableConds(q::ACSets.Query.ACSetSQLNode)
    result = Dict{Vector{Symbol}, Vector{WhereCondition}}()
    walk = @λ begin
        wheres::Vector{ACSets.Query.AbstractCondition} -> walk.(wheres)
        boolean::Union{AndWhere, OrWhere} -> walk.(boolean.conds)
        wc::WhereCondition -> begin
            out = Symbol[]
            lhs = (length(wc.lhs) > 1 && !(wc.lhs isa String)) ? push!(out, wc.lhs[1]) : nothing
            rhs = (length(wc.rhs) > 1 && !(wc.rhs isa String)) ? push!(out, wc.rhs[1]) : nothing
            haskey(result, out) ? setindex!(result, [result[out]; [wc]], out) : push!(result, out => [wc])
        end
        _ => nothing 
    end
    walk(q.cond)
    TableConds(result)
end
## render to 

# TODO have table alias
function render(source::DBSource{SQLite.DB}, wc::WhereCondition)
    "$(wc.lhs[1]).$(wc.lhs[2]) = $(to_sql(source, wc.rhs))"
end

function ACSetInterface.incident(fabric::DataFabric, wc::WhereCondition)
    incident(fabric, Symbol(wc.rhs), wc.lhs[2])
end

@present SchQueryGraph <: SchLabeledGraph begin
    Data::AttrType
    data::Attr(V, Data)
end
@acset_type QueryGraph(SchQueryGraph)

using StructEquality

@struct_hash_equal struct Field
    x
end

function Fabric.execute!(fabric::DataFabric, q::ACSets.Query.ACSetSQLNode)
    qg = QueryGraph{Field, Any}()
    d = TableConds(q)
    # query independent tables
    tables = filter(x -> length(x) == 1, keys(d.conds))
    # get their results. we assume "OR" right now
    for table in tables
        add_part!(qg, :V, label=Field([only(table), :id]), data=collect(Iterators.flatten(incident.(Ref(fabric), d.conds[table]))))
    end
    # if a key is an adhesion, then query the easiest one, 
    # and pass the ids in as a where statement
    adh_tables = filter(x -> length(x) == 2, keys(d.conds))
    # query each adhesion
    for table in adh_tables
        rhs = getfield.(d.conds[table], :rhs)
        lhs = getfield.(d.conds[table], :lhs)
        ids = subpart(qg, incident(qg, Field(first(rhs)), :label), :data)
        add_part!(qg, :V, label=Field(table), data=incident(fabric, only(ids), first(lhs)[2]))
        # add_part!(qt, :E, src=[], tgt=[])
        # add_part!(qg, :E, src=
        # table => subpart(fabric, df._id, :country)
    end
    # get the table associated to the righthand WhereCondition
    # select the RHS.col where the ids agree
    # SELECT
    # using the select fields, go to the node and select the columns with it
    qg
end

execute!(fabric, q)

execute!(fabric,
"""
from Winemaker w
left join Country c
on w.country_code = c.id
where c.country = "France"
select w.winemaker
"""


