# blends homs and attrs together. not ideal
function namesrctgt(schema::BasicSchema)
    Dict([name => src => tgt for (name, src, tgt) in schema.homs ∪ schema.attrs])
end

struct PagingInfo 
    startIndex::Int
    batchSize::Union{Int, Nothing}
end

function getattrs(g::ACSet, table::Symbol)
    first.(filter(attrs(acset_schema(g))) do (attr, tbl, _)
        table == tbl
    end)
end
export getattrs

gethoms(x::ACSet, table::Symbol) = first.(homs(acset_schema(x); from=table))
export gethoms

# FIXME Set
function colnames(x::ACSet, table::Symbol)
    homnames = first.(homs(acset_schema(x); from=table))
    gattrs = getattrs(x, table)
    # I don't like this as it assumes the order of the columns would agree
    cols = [:_id, (homnames ∪ gattrs)...]
    """($(join(cols, ", ")))"""
end
export colnames

function wrap(stmt::String, left::String, right::String)
    join([left, stmt, right], " ")
end
export wrap

p = @present Something(FreeSchema) begin
    ob::Ob
    t::AttrType
    v::Attr(ob, t)
end

# i assume freeschema
function shunt(p::Presentation)
    n = (at -> Symbol("obj_$(nameof(at))"))
    q = Presentation(FreeSchema)
    obs = p.generators.Ob
    add_generators!(q, obs)
    # add original homs
    homs = p.generators.Hom
    add_generators!(q, homs)
    # add the attrtypes to objects
    ats = p.generators.AttrType
    add_generators!(q, ats)
    attrdict = Dict(at => add_generator!(q, Ob(FreeSchema.Ob, n(at))) for at in ats)
    # for every attr, create a hom to the attr-ob and create an attr from the attr-ob
    for attr in p.generators.Attr
        newhom = Hom(gensym(nameof(attr)), dom(attr), attrdict[codom(attr)])
        add_generator!(q, newhom)
        # Attr(:v, ob, t) -> Attr(:v, g(ob), t) 
        add_generator!(q, Attr(nameof(attr), attrdict[codom(attr)], codom(attr)))
    end
    return q
end

q = shunt(p)
@acset_type NewType2(q)



function Matrix(d::Dict{Symbol, Encoded})
    hcat(getfield.(values(d), Ref(:encoded))...) 
end

