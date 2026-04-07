using FunSQL
using FunSQL: render, reflect

import ACSets.Query: WhereCondition

# TODO have table alias
# function FunSQL.render(source::DBSource{SQLite.DB}, wc::WhereCondition)
#     "$(wc.lhs[1]).$(wc.lhs[2]) = $(sql(source, wc.rhs))"
# end

""" Dispatches FunSQL's render SQLNodes """
function FunSQL.render(source::DBSource{SQLite.DB}, n::FunSQL.SQLNode)
    FunSQL.render(source.conn, n)
end

# Render functions convert our intermediate syntax (Insert, etc.) into
# strings in the target DB dialect.

function FunSQL.render(source::DBSource{SQLite.DB}, i::Insert)
    cols = join(columns(i.values), ", ")
    # TODO what is the "key" doing here
    values = join(["($x)" for x ∈ sql.(Ref(source), i.values.vals; key=false)], ", ")
    "INSERT OR IGNORE INTO $(i.table) ($cols) VALUES $values ;"
end
export render

function FunSQL.render(source::DBSource{SQLite.DB}, u::Update) 
    cols = join(columns(u.values), ", ")
    wheres = !isnothing(u.wheres) ? render(source, u.wheres) : ""
    "UPDATE $(u.table) SET $(sql(source, u.values)) " * wheres * ";"
end

# TODO might have to refactor so we can reuse code for show method
function FunSQL.render(source::DBSource{SQLite.DB}, s::SQL.Syntax.Select)
    from = s.from isa Vector ? join(s.from, ", ") : s.from
    qty = render(source, s.qty)
    join = !isnothing(s.join) ? render(source, s.join) : " "
    wheres = !isnothing(s.wheres) ? render(source, s.wheres) : ""
    "SELECT $qty FROM $from " * join * wheres * ";"
end

function FunSQL.render(source::DBSource{SQLite.DB}, j::Join)
    "$(j.type) JOIN $(j.table) ON $(render(source, j.on))"
end

function FunSQL.render(source::DBSource{SQLite.DB}, ons::Vector{Equation})
    join(render.(Ref(source), ons), " AND ")
end

function FunSQL.render(source::DBSource{SQLite.DB}, eq::Equation)
    "$(eq.lhs.first).$(eq.rhs.second) = $(eq.rhs.first).$(eq.rhs.second)"
end

function FunSQL.render(source::DBSource{SQLite.DB}, qty::SelectQuantity)
    @match qty begin
        ::SelectAll || ::SelectDistinct || ::SelectDistinctRow => "*"
        SelectColumns(cols) => join(render.(Ref(source), cols), ", ")
    end
end

function FunSQL.render(::DBSource{SQLite.DB}, column::Union{Pair{Symbol, Symbol}, Symbol})
    @match column begin
        ::Pair{Symbol, Symbol} => "$(column.first).$(column.second)"
        _ => column
    end
end

# TODO
function FunSQL.render(::DBSource{SQLite.DB}, wheres::WhereClause)
    @match wheres begin
        WhereClause(op, d::Pair) => "WHERE $(d.first) $op ($(join(d.second, ", ")))"
        _ => wheres
    end
end

function FunSQL.render(source::DBSource, c::Create)
    create_stmts = map(objects(c.schema)) do ob
        obattrs = attrs(c.schema; from=ob)
        "CREATE TABLE IF NOT EXISTS $(ob)(" * 
            join(filter(!isempty, ["_id INTEGER PRIMARY KEY",
                # column_name column_type
                join(map(homs(c.schema; from=ob)) do (col, src, tgt)
                       tgttype = sql(source, Int)
                       "$(col) $tgttype"
                end, ", "),
                join(map(obattrs) do (col, _, type)
                    # FIXME
                    "$(col) $(sql(source, subpart_type(source.acsettype(), type)))" 
               end, ", ")]), ", ") * ");"
    end
    join(create_stmts, " ")
end

function FunSQL.render(source::DBSource, t::ACSet)
    s = acset_schema(t)
    stmts = map(objects(s)) do ob
        obattrs = attrs(s; from=ob)
        "CREATE TABLE IF NOT EXISTS $ob(" *
        join(filter(!isempty, ["_id INTEGER PRIMARY KEY",
            join(map(homs(s; from=ob)) do (col, _, _)
                tgttype = sql(source, Int)
                "$col $tgttype"
            end, ", "),
            join(map(obattrs) do (col, src, tgt)
                "$col $(sql(source, subpart_type(t, tgt)))"
            end, ", ")
           ]), ", ") * ");"
    end
    join(stmts, " ")
end

function FunSQL.render(source::DBSource{SQLite.DB}, d::Delete)
    "DELETE FROM $(d.table) WHERE _id IN ($(join(d.ids, ",")))"
end

function FunSQL.render(::DBSource{SQLite.DB}, v::Values)
    "VALUES " * join(entuple(v), ", ") * ";"
end

function FunSQL.render(::DBSource{SQLite.DB}, a::Alter)
    "ALTER TABLE $(a.refdom) ADD CONSTRAINT fk_$(ref) FOREIGN KEY ($(a.ref)) REFERENCES $(a.refcodom)(_id); "
end

function FunSQL.render(::DBSource{SQLite.DB}, ::ShowTables)
    "SELECT name FROM sqlite_master WHERE type='table';"
end

function FunSQL.render(::DBSource{SQLite.DB}, fkc::ForeignKeyChecks)
    "SET FOREIGN_KEY_CHECKS = $(Int(fkc.bool)) ;"
end

