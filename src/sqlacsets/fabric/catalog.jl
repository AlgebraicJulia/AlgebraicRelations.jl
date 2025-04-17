# CATALOG

(<|)(x, y) = something(x, y)
(<|)(x::AbstractArray, y) = begin 
    @info x, y, isempty(x)
    isempty(x) ? y : x
end

@present SchERD(FreeSchema) begin
    (Name, Conn, SourceId)::AttrType
    (Source, Table, Column, FK)::Ob

    (to, from)::Hom(FK, Column)
    table::Hom(Column, Table)
    source::Hom(Table, Source)

    tname::Attr(Table, Name)
    (type, cname)::Attr(Column, Name)
    source_id::Attr(Source, SourceId)
    conn::Attr(Source, Conn)
end
@abstract_acset_type AbstractSQLSchema
@acset_type ERD(SchERD) <: AbstractSQLSchema

const Catalog = ERD{Symbol, DataType, Int}
export Catalog

function (c::Catalog)(value)
    # TODO pointer to object
    add_part!(c, :Source, conn=Memory(value))
    c
end

function add_to_catalog!(catalog::Catalog, p::SQLSchema; source=nothing, conn=nothing, types::Union{Dict, Nothing}=nothing)
    @info p
    fields = get_fields(p, types)
    tables = keys(fields)
    id = :SERIAL_PRIMARY_KEY # TODO PostgreSQL
    fk = :INTEGER
    tab2ind = Dict{Symbol, Int64}()
    # load tables into their relations
    source_id = if !isnothing(source)
        if isnothing(subpart(catalog, source, :source_id))
            add_part!(catalog, :Source, source_id=source, conn=conn)
        else
            subpart(catalog, source, :source_id)
        end
        else
            0
        end
    for t in tables
        # check if the table is in the catalog, otherwise add it
        t_ind = if isempty(incident(catalog, t, :tname))
            add_part!(catalog, :Table, tname=t, source=source_id)
        else
            incident(catalog, t, :tname)
        end
        # c_ind = something(incident(catalog, t_ind, :table), add_part!(catalog, :Column, table=only(t_ind), cname=:id, type=id))
        tab2ind[t] = only(t_ind)
    end
    #
    for t in tables
      for c in fields[t]
        if c[1] == :Hom
            col = if isempty(incident(catalog, c[3], :cname))
                add_part!(catalog, :Column, table=tab2ind[t], cname=c[3], type=fk)
            else
                only(incident(catalog, c[3], :cname))
            end
            if isempty(incident(catalog, col, :from)) && isempty(incident(catalog, tab2ind[c[2]], :to)) 
                add_part!(catalog, :FK, from=col, to=tab2ind[c[2]])
            end
        else
            type = type2sql(c[2])
            if isempty(incident(catalog, c[3], :cname))
                add_part!(catalog, :Column, table = tab2ind[t], cname = c[3], type=Symbol(type))
            else
                only(incident(catalog, c[3], :cname))
            end
        end
      end
    end
    catalog
end
export add_to_catalog!
