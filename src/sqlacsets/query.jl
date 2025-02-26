import FunSQL: From, Select, Where

mutable struct SQLACSetNode
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
From(table::◊Ob) = SQLACSetNode(table.x, [], Symbol[])

function From(sql::SQLACSetNode; table::◊Ob)
    sql.from = [sql.from; table.x]
    sql
end
export From

function Where(sql::SQLACSetNode; lhs::Symbol, op::Symbol, rhs::Any)
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

function Select(sql::SQLACSetNode; columns::Union{Symbol, Vector{Symbol}})
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

"""
A query with no select overly-specified:
```
q = From(◊Ob(:Summand))
```
A query with the select specified. `_id` is reserved for the part.
```
q = From(◊Ob(:Summand)) |>
Select(◊Ob(:_id))
```
A query with two where statements. One Where uses another query
```
q = From(◊Ob(:Op1)) |>
Where(:src, :∈, invasion[:res] ∪ invasion[:sum] ∪ infer_states(invasion)) |>
Where(:src, :∈, From(◊Ob(:Op1)) |> Where(:op1, :∈, blacklist) |> Select(◊Ob(:tgt)))
```
"""
function (q::SQLACSetNode)(acset::ACSet)
    # decided OR for now
    idx = @match process_wheres(q, acset) begin
        ::Nothing => nothing
        indices => length(indices) > 1 ? [l || r for (l,r) ∈ zip(indices...)] : only(indices)
    end
    result = isnothing(idx) ? parts(acset, q.from) : parts(acset, q.from)[idx]
    @match q.select begin
        ::Nothing || [:_id] => return result
        selects => [result, subpart.(Ref(acset), filter(x -> x != :_id, selects))...]
    end
end

function process_wheres(q::SQLACSetNode, acset::ACSet)
    isempty(q.cond) && return nothing
    whereindices = map(q.cond) do (left, _, right)
        @match right begin
            ::SQLACSetNode => acset[left] .∈ right(acset)
            _ => map(x -> right isa Vector ? x ∈ right : x == right, acset[left])
        end
    end
end

DBInterface.execute(acset::ACSet, q::SQLACSetNode) = q(acset)
