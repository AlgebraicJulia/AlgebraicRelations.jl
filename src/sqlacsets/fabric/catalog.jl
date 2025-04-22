# CATALOG

(<|)(x, y) = something(x, y)
(<|)(x::AbstractArray, y) = begin 
    isempty(x) ? y : x
end

emptyMaybe(x) = isempty(x) ? nothing : Some(x)

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
    add_part!(c, :Source, conn=Memory(value))
    c
end

function table_to_fields(s::SQLSchema{T}) where T
    Dict([ 
          Symbol(subpart(s, table, :tname)) => map(incident(s, table, :table)) do column
                if subpart(s, column, :from) == 0 # not a foreign key
                    (:Attr, Symbol(subpart(s, table, :tname)), Symbol(subpart(s, column, :cname)))
                else
                    tgttable = subpart(s, subpart(s, subpart(s, column, :to), :table), :tname)
                    (:Hom, Symbol(tgttable), Symbol(subpart(s, subpart(s, column, :from), :cname)))
                end
          end
    for table in parts(s, :Table) 
    ]) 
end

function add_to_catalog!(catalog::Catalog, s::SQLSchema{T}; source=nothing, conn=nothing, types::Union{Dict, Nothing}=nothing) where T
    id = :SERIAL_PRIMARY_KEY # TODO PostgreSQL
    fk = :INTEGER
    # load tables into their relations
    source_id = if !isnothing(source)
        @something subpart(catalog, source, :source_id) add_part!(catalog, :Source, source_id=source, conn=conn)
        else
            0
        end
    foreach(subpart(s, :, :tname)) do table
        @something emptyMaybe(incident(catalog, Symbol(table), :tname)) add_part!(catalog, :Table, tname=Symbol(table), source=source_id)
    end
    foreach(subpart(s, :, :cname)) do column
        # get the id of the table
        sch_cid = incident(s, column, :cname)
        sch_tname = subpart(s, subpart(s, sch_cid, :table), :tname) |> only
        catalog_tid = incident(catalog, Symbol(sch_tname), :tname)
        @something emptyMaybe(incident(catalog, Symbol(column), :cname)) add_part!(catalog, :Column, table=only(catalog_tid), cname=Symbol(column), type=:INTEGER)
    end
    foreach(parts(s, :FK)) do fk
        from = subpart(s, subpart(s, fk, :from), :cname)
        to = subpart(s, subpart(s, fk, :to), :cname)
        c_fromid = incident(catalog, Symbol(from), :cname) |> only
        c_toid = incident(catalog, Symbol(to), :cname) |> only
        if isempty(incident(catalog, c_toid, :to) ∩ incident(catalog, c_fromid, :from))
            add_part!(catalog, :FK, to=c_toid, from=c_fromid)
        end
    end
    catalog
end
export add_to_catalog!
