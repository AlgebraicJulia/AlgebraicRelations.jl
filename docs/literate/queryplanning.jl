using Catlab, ACSets
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

@present SchWinemaker(FreeSchema) begin
    (Name, Country)::AttrType
    Winemaker::Ob
    country_code::Attr(Winemaker, Country)
    wm_name::Attr(Winemaker, Name) # TODO "name" does not get entered
    # fk constraint means that there is *some* schema out there
end
@acset_type Winemaker(SchWinemaker)
winemakers = Winemaker{Symbol, FK{Country}}()
winemaker_db = DBSource(SQLite.DB(), acset_schema(winemakers))

import FunSQL: render
execute!(winemaker_db, render(winemaker_db, winemakers))

execute!(winemaker_db, "pragma table_info(Winemaker)")

winemaker_src = add_source!(fabric, winemaker_db)
add_fk!(fabric, winemaker_src, country_src, :Winemaker!country_code => :Country!Country_id)

reflect!(fabric)

add_part!(fabric, :Country, country=:Italy, code=:IT)
add_part!(fabric, :Country, country=:France, code=:FR)
add_part!(fabric, :Country, country=:USA, code=:US)

subpart(fabric, :country)

# TODO does not guard against constraint
add_part!(fabric, :Winemaker, [(_id=1, country_code=1, wm_name=:Banfi)
                               ,(_id=2, country_code=2, wm_name=:NotBanfi)
                               ,(_id=3, country_code=2, wm_name=:BanfiAsWell)
                               ,(_id=4, country_code=3, wm_name=:AmericanBanfi)])

execute!(winemaker_db, "select * from Winemaker")

using MLStyle: @match, @λ
using ACSets.Query
import ACSets.Query: WhereCondition, AndWhere, OrWhere

# TODO add the Join statement to track the first where
q = From(:Winemaker) |> Where([:Winemaker, :country_code], ==, [:Country, :id]) |>
                        Where([:Country, :country], ==, "France")

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

d = execute!(q)

# TODO have table alias
function render(source::DBSource{SQLite.DB}, wc::WhereCondition)
    "$(wc.lhs[1]).$(wc.lhs[2]) = $(to_sql(source, wc.rhs))"
end

function ACSetInterface.incident(fabric::DataFabric, wc::WhereCondition)
    incident(fabric, Symbol(wc.rhs), wc.lhs[2])
end

function Fabric.execute!(fabric::DataFabric, q::ACSets.Query.ACSetSQLNode)
    d = TableConds(q)
    # query independent tables
    tables = filter(x -> length(x) == 1, keys(d.conds))
    # get their results. we assume "OR" right now
    itr = Dict(Iterators.map(tables) do table
        [only(table), :id] => collect(Iterators.flatten(incident.(Ref(fabric), d.conds[table])))
    end)
    @info itr
    # if a key is an adhesion, then query the easiest one, 
    # and pass the ids in as a where statement
    adh_tables = filter(x -> length(x) == 2, keys(d.conds))
    #
    tablefields = Iterators.map(adh_tables) do table
        rhs = getfield.(d.conds[table], :rhs)
        lhs = getfield.(d.conds[table], :lhs)
        # itr[first(rhs)] gets the ids from the `itr` variable
        # this returns the _ids of the matchs
        df = incident(fabric, itr[first(rhs)], first(lhs)[2])
        # @info df, first(lhs)[2]
        table => df
        # table => subpart(fabric, df._id, :country)
    end |> collect
    # get the table associated to the righthand WhereCondition
    # select the RHS.col where the ids agree
end

execute!(fabric, q)

execute!(fabric,
"""
from Winemaker w
left join Country c
on w.country_code = c.id
where c.country = "France"
select w.wm_name
"""


