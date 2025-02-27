import FunSQL: From, Select, Where

abstract type AbstractCondition end
export AbstractCondition

@kwdef struct Cond <: AbstractCondition
    lhs::Symbol
    op::Symbol = :∈
    rhs::Any
end
export Cond

# TODO what about OR?
@as_record struct AndWhere <: AbstractCondition
	conds::Vector{<:AbstractCondition}
	# constructors
	AndWhere(a::AndWhere, b) = new(a.conds, b)
	AndWhere(a, b::AndWhere) = new(a, b.conds)
	AndWhere(a::AbstractCondition, b::AbstractCondition) = new([a, b])
	AndWhere(a::Vector{<:AbstractCondition}, b::Vector{<:AbstractCondition}) = new([a.cond; b.conds])
	AndWhere(conds::Vector{<:AbstractCondition}) = new(conds)
end

function Base.:&(a::S, b::T) where {T<:AbstractCondition, S<:AbstractCondition}
	AndWhere(a, b)
end

function Base.:&(n::SQLACSetNode, a::AbstractCondition)
	n.cond = n.cond & a
	n
end

@as_record struct OrWhere <: AbstractCondition
	conds::Vector{<:AbstractCondition}
	# constructors
	OrWhere(a::OrWhere, b) = new(a.conds, b)
	OrWhere(a, b::OrWhere) = new(a, b.conds)
	OrWhere(a::AbstractCondition, b::AbstractCondition) = new([a, b])
	OrWhere(a::Vector{<:AbstractCondition}, b::Vector{<:AbstractCondition}) = new([a.cond; b.conds])
	OrWhere(conds::Vector{<:AbstractCondition}) = new(conds)
end

function Base.:|(a::S, b::T) where {T<:AbstractCondition, S<:AbstractCondition}
    OrWhere(a, b)
end

function Base.:|(n::SQLACSetNode, a::AbstractCondition)
    n.cond = n.cond | a
    n
end

mutable struct SQLACSetNode
    from::Symbol
    cond::Union{Vector{<:AbstractCondition}, Nothing}
    select::Union{Symbol, Vector{Symbol}, Nothing}
    SQLACSetNode(from::Symbol; cond=nothing, select=nothing) = new(from, cond, select)
end
export SQLACSetNode

function (w::Cond)(node::SQLACSetNode)
	push!(node.cond, AndWhere([w]))
	node
end

function (ac::AbstractCondition)(node::SQLACSetNode)
	push!(node.cond, ac)
	node
end

struct ◊Ob
    x::Symbol
end
export ◊Ob

◊Ob(xs...) = ◊Ob.([xs...])

# TODO handle nothing case
From(table::◊Ob) = SQLACSetNode(table.x; cond=AbstractCondition[], select=Symbol[])

# TODO looks like we don't do this anymore. From is singleton for the time being.
function From(sql::SQLACSetNode; table::◊Ob)
    sql.from = [sql.from; table.x]
    sql
end
export From

# function Cond(sql::SQLACSetNode; lhs::Symbol, op::Symbol=:∈, rhs::Any)
#     sql.cond = if !isnothing(sql.cond)
#         [sql.cond; Cond(lhs, op, rhs)]
#     else
#         [Cond(lhs, op, rhs)]
#     end
#     sql
# end
# export Where

function Where(lhs::Symbol, op::Symbol, rhs::Any)
	Cond(lhs=lhs, op=op, rhs=rhs)
end
Where(lhs::Symbol, rhs::Any) = Where(lhs, :∈, rhs)
export Where

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

# ##

function process_wheres end
export process_wheres

function process_wheres(conds::Vector{<:AbstractCondition}, acset)
	isempty(conds) && return nothing
	process_wheres.(conds, Ref(acset))
end

function process_wheres(cond::Cond, acset::ACSet)
    schema = acset_schema(acset)
    values = cond.lhs ∈ objects(schema) ? parts(acset, cond.lhs) : acset[cond.lhs]
    @match cond.rhs begin
        ::SQLACSetNode => map(values) do x; x ∈ cond.rhs(acset) end
        _ => map(x -> cond.rhs isa Vector ? x ∈ cond.rhs : x == cond.rhs, values)
    end
end

# function process_wheres(conds::Vector{Cond}, acset)
# 	isempty(conds) && return nothing
# 	process_wheres.(conds, Ref(acset))
# end

function process_wheres(w::OrWhere, acset::ACSet)
    isempty(w.conds) && return nothing
    reduce((x,y) -> x .| y, process_wheres(w.conds, acset))
end

function process_wheres(w::AndWhere, acset::ACSet)
    isempty(w.conds) && return nothing
    reduce((x,y) -> x .& y, process_wheres(w.conds, acset))
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
    idx = process_wheres(q.cond, acset)
    result = isnothing(idx) ? parts(acset, q.from) : parts(acset, q.from)[first(idx)]
    isempty(result) && return []
    selected = @match q.select begin
        ::Nothing || Symbol[] || [:_id] || :_id => return result
        ::Symbol => subpart(acset, result, q.select)
        selects => map(selects) do select
            subpart(acset, select)[result]
        end
    end
    collect(Iterators.flatten(selected))
end

DBInterface.execute(acset::ACSet, q::SQLACSetNode) = q(acset)
