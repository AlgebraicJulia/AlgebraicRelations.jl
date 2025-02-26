import FunSQL: From, Select, Where

mutable struct ACSetSQLNode
    from::Symbol
    cond::Union{Vector{Tuple{Symbol, Symbol, Any}}, Nothing}
    select::Union{Symbol, Vector{Symbol}, Nothing}
end

struct ◊Ob
    x::Symbol
end
export ◊Ob

◊Ob(xs...) = ◊Ob.([xs...])

# TODO handle nothing case
From(table::◊Ob) = ACSetSQLNode(table.x, nothing, Symbol[])

function From(sql::ACSetSQLNode; table::◊Ob)
    sql.from = [sql.from; table.x]
    sql
end
export From

function Where(sql::ACSetSQLNode; lhs::Symbol, op::Symbol, rhs::Any)
    sql.cond = if !isnothing(sql.cond)
        [sql.cond; (lhs, op, rhs)]
    else
        [(lhs, op, rhs)]
    end
    sql
end
export Where

function Where(lhs::Symbol, op::Symbol, rhs::Any)
    sql -> Where(sql; lhs=lhs, op=op, rhs=rhs)
end

function Select(sql::ACSetSQLNode; columns::Union{Symbol, Vector{Symbol}})
    push!(sql.select, columns...)
    sql
end
export Select

function Select(cols::Union{◊Ob, Vector{◊Ob}})
    if cols isa Vector
        sql -> Select(sql; columns=getfield.(cols, :x))
    else
        sql -> Select(sql; columns=[cols.x])
    end
end

function (q::ACSetSQLNode)(acset::ACSet)
    indices = map(q.cond) do (left, _, right)
        @match right begin
            ::ACSetSQLNode => acset[left] .∈ right(acset)
            _ => map(x -> right isa Vector ? x ∈ right : x == right, acset[left])
        end
    end
    # decided OR for now
    idx = length(indices) > 1 ? [l || r for (l,r) ∈ zip(indices...)] : only(indices)
    result = parts(acset, q.from)[idx]
end


# using MLStyle

# abstract type SQLACSetNode end

# @as_record struct ◊From <: SQLACSetNode
#     table::Symbol
#     sql::Union{SQLACSetNode, Nothing}
#     ◊From(table::Symbol) = new(table, nothing)
#     ◊From(table, sql) = new(table, sql)
# end
# export ◊From

# struct _WhereClause
#     op::Symbol
#     lhs::Symbol
#     rhs::Union{Symbol, <:SQLACSetNode}
# end

# @as_record struct ◊Where <: SQLACSetNode
#     op::Union{Symbol, Nothing}
#     ws::Union{_WhereClause, Vector{_WhereClause}}
#     sql::Union{<:SQLACSetNode, Nothing}
#     function ◊Where(ws::Union{_WhereClause, Vector{_WhereClause}}; 
#             op=nothing, sql=nothing)
#         new(op, ws, sql)
#     end
#     function ◊Where(op::Symbol, lhs, rhs)
#         new(nothing, _WhereClause(op, lhs, rhs), nothing)
#     end
# end
# export ◊Where

# @as_record struct ◊Select <: SQLACSetNode 
#     columns::Union{Symbol, Vector{Symbol}, Nothing}
#     ◊Select(x) = new(x)
#     ◊Select() = new(nothing)
# end
# export ◊Select

# (x::◊From)(q::SQLACSetNode) = q
# (x::◊From)(::Nothing) = x

# function (w::◊Where)(q::Union{SQLACSetNode, Nothing})
#     @match q begin
#         ::◊From => ◊From(q.table, w(q.sql))
#         # terminate at _Where
#         ::◊Where => ◊Where([w.ws; q.ws]; op=w.op, sql=q.sql)
#         ::◊Select => q(w)
#         ::Nothing => w
#     end
# end

# # select goes to the back
# function (s::◊Select)(q::Union{SQLACSetNode, Nothing}) 
#     @match q begin
#         ::◊From => ◊From(q.table, s(q.sql))
#         ::◊Where => ◊Where(q.ws; op=q.op, sql=s(q.sql))
#         ::◊Select => ◊Select([q.columns; s.columns]) # combine
#         _ => s
#     end
# end

# ◊From(:D) |> ◊Where(:in, :src, :tgt) |> ◊Select(:a) |> ◊Select(:b)

# ◊From(:D) |> ◊Where(:in, :src, :tgt)

# # FROM d WHERE
# #   d.src ∈ filter(!isnothing, infer_states(d)) ||
# #   d.src ∈ d[:res] ||
# #   d.src ∈ d[:sum] ||
# #   d.src ∈ d[(from d where d.op1 ∈ blacklist select d.id), :tgt]
# # select distinct d.id

# q = ◊From(:d) |>
#     ◊Where(:src, :in, :val) |>
#     ◊Where(:src, :in, :res) |>
#     ◊Where(:src, :in, :sum) |>
#     ◊Where(:src, :in, ◊From(:d) |>
#            ◊Where(:tgt, :in, :placeholder) |>
#            ◊Select(:_id)) |>
#     ◊Select(:_id);

# function (q::SQLACSetNode)(acset::ACSet)
#     @match q begin
#         ◊From(table, 
#         ◊Where(op, ws, 
#         ◊Select(cols))) => ws
#         _ => q
#     end
#     parts(acset, table)[
# end
