# CATALOG

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

function add_to_catalog!(catalog::Catalog, p::Presentation; source=nothing, conn=nothing, types::Union{Dict, Nothing}=nothing)
    fields = get_fields(p, types)
    tables = keys(fields)
    id = :SERIAL_PRIMARY_KEY # TODO PostgreSQL
    fk = :INTEGER
    tab2ind = Dict{Symbol, Int64}()
    # load tables into their relations
    source_id = !isnothing(source) ? add_part!(catalog, :Source, source_id=source, conn=conn) : 0
    for t in tables
        # TODO upstream
        t_ind = incident(catalog, t, :tname)
        t_ind = isempty(t_ind) ? add_part!(catalog, :Table, tname=t, source=source_id) : only(t_ind)
        c_ind = incident(catalog, t_ind, :table)
        c_ind = isempty(c_ind) ? add_part!(catalog, :Column, table=t_ind, cname=:id, type=id) : only(c_ind)
      tab2ind[t] = t_ind
    end
    #
    for t in tables
      for c in fields[t]
        if c[1] == :Hom
          col = add_part!(catalog, :Column, table = tab2ind[t], cname = c[3], type=fk)
          add_part!(catalog, :FK, from=col, to=tab2ind[c[2]])
        else
          type = type2sql(c[2])
          add_part!(catalog, :Column, table = tab2ind[t], cname = c[3], type=Symbol(type))
        end
      end
    end
    catalog
end
export add_to_catalog!

