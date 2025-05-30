# activate the Project.toml at the root dir
using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
using Catlab.WiringDiagrams.RelationDiagrams: UntypedNamedRelationDiagram

include("examples/wineries.jl");

# fabric.graph
# fabric.catalog

using DataFrames

"""
A schema to describe SQL schemas.
`Table` is what you would expect. `Column` are the columns of a table, so they always know who they belong to col_of.
PK is the primary key of a table pk_of. It needs a junction table PK_Cols because composite primary keys need
to reference multiple columns of a table. FK are foreign keys, they take you from one table to a PK (of another table).
Because the referenced primary key may be composite, the foreign key may also need to be composite, so there is a junction
table FK_Cols to relate a foreign key to its columns.

`Source` tells us what type of data source is used as the backend for each `Table`. The hom `source` should really be a
bijection, but the best we can do is use `unique_index` to assert its injective. The reason we introduce a new Ob rather
that have `conn` and `source_id` directly come from `Table` is we might want to swap data sources without touching
the conceptual schema.

There are 2 commuting squares. The first is defined by pk_col; col_of == pk; pk_of, saying that a primary key's columns
must belong to the table that it is a primary key for. The second is analogous for foreign keys and is defined by fk; from == fk_col; col_of.
We would also like to say that from != to; pk_of so that foreign keys have to go between different tables but I do not know
how to enforce that.

We set injective constraints (via `unique_index` when calling `@acset_type`) on the following homs:
  * `pk_of`: each primary key can only be the PK of a single table
"""
@present TheorySQLSchema(FreeSchema) begin
  (Table, Source, Column, PK, PK_Cols, FK, FK_Cols)::Ob
  col_of::Hom(Column, Table)
  pk_col::Hom(PK_Cols, Column)
  pk::Hom(PK_Cols, PK)
  pk_of::Hom(PK, Table)
  to::Hom(FK, PK)
  from::Hom(FK, Table)
  fk::Hom(FK_Cols, FK)
  fk_col::Hom(FK_Cols, Column)
  Name::AttrType
  tab_name::Attr(Table, Name)
  col_name::Attr(Column, Name)
  col_type::Attr(Column, Name)
  source::Hom(Table,Source)
  Conn::AttrType
  SourceID::AttrType
  conn::Attr(Source,Conn)
  source_id::Attr(Source,SourceID)
  compose(pk, pk_of) == compose(pk_col, col_of)
  compose(fk_col, col_of) == compose(fk, from)
  # compose(to, pk_of) != from
end

# to_graphviz(TheorySQLSchema, graph_attrs=Dict(:size=>"7.5",:ratio=>"expand"))
# to_graphviz(AlgebraicRelations.Fabric.SchERD, graph_attrs=Dict(:size=>"4.5",:ratio=>"expand"))

@abstract_acset_type AbstractSQLSchema

@acset_type _SQLSchema(
    TheorySQLSchema, 
    index=[:col_of, :pk_col, :pk, :to, :from, :fk, :fk_col], 
    unique_index=[:pk_of, :source]
) <: AbstractSQLSchema

sch_acs = @acset _SQLSchema{Symbol, DataType, Int} begin
    Source=nparts(fabric.catalog, :Source)
    conn=fabric.catalog[:, :conn]
    source_id=fabric.catalog[:, :source_id]
    #
    Table=nparts(fabric.catalog, :Table)
    tab_name=fabric.catalog[:, :tname]
    source=fabric.catalog[:, :source]
    #
    Column=nparts(fabric.catalog, :Column)
    col_name=fabric.catalog[:, :cname]
    col_type=[x ∈ [:PK,:FK] ? :Integer : x for x in nameof.(fabric.catalog[:, :type])]
    col_of=fabric.catalog[:, :table]
end

# add PKs
for pk_col in incident(fabric.catalog, PK, :type)
    pk = add_part!(sch_acs, :PK, pk_of=fabric.catalog[pk_col, :table])
    add_part!(sch_acs, :PK_Cols, pk=pk, pk_col=pk_col)
end

# add FKs
for fk in parts(fabric.catalog, :FK)
    from = fabric.catalog[fk, (:from, :table)]
    catalog_to_table = fabric.catalog[fk, (:to, :table)]
    to = only(incident(sch_acs, catalog_to_table, :pk_of))
    fk_id = add_part!(sch_acs, :FK, from=from, to=to)
    add_part!(sch_acs, :FK_Cols, fk=fk_id, fk_col=fabric.catalog[fk, :from])
end

"""
Temporary helper fn to take catalog and make a _SQLSchema acset
"""
function catalog_to_SQLSchema(catalog)
    sch_acs = @acset _SQLSchema{Symbol, DataType, Int} begin
        Source=nparts(catalog, :Source)
        conn=catalog[:, :conn]
        source_id=catalog[:, :source_id]
        #
        Table=nparts(catalog, :Table)
        tab_name=catalog[:, :tname]
        source=catalog[:, :source]
        #
        Column=nparts(catalog, :Column)
        col_name=catalog[:, :cname]
        col_type=[x ∈ [:PK,:FK] ? :Integer : x for x in nameof.(catalog[:, :type])]
        col_of=catalog[:, :table]
    end

    # add PKs
    for pk_col in incident(catalog, PK, :type)
        pk = add_part!(sch_acs, :PK, pk_of=catalog[pk_col, :table])
        add_part!(sch_acs, :PK_Cols, pk=pk, pk_col=pk_col)
    end

    # add FKs
    for fk in parts(catalog, :FK)
        from = catalog[fk, (:from, :table)]
        catalog_to_table = catalog[fk, (:to, :table)]
        to = only(incident(sch_acs, catalog_to_table, :pk_of))
        fk_id = add_part!(sch_acs, :FK, from=from, to=to)
        add_part!(sch_acs, :FK_Cols, fk=fk_id, fk_col=catalog[fk, :from])
    end
    return sch_acs
end

"""
A dictionary to map the type of a column to an emoji
"""
const schema_col_emoji = Dict(
    "pk" => "🔑",
    "pk/fk" => "🔑🔗",
    "fk" => "🔗",
    "data" => "📊"
)

"""
For a table with part ID `tab_id` get a dataframe
that contains columns necessary to generate the table cells of the HTML node label
"""
function get_cols_table(acs::T, tab_id) where {T<:AbstractSQLSchema}
    # all deepcopys can be replaced when figure out ACSets.jl issue
    tab_cols = deepcopy(incident(acs, tab_id, :col_of))    
    tab_pk = acs[incident(acs, tab_id, (:pk, :pk_of)), :pk_col]
    tab_fk = acs[incident(acs, tab_id, (:fk, :from)), :fk_col]
    tab_pk_fk = intersect(tab_fk, tab_pk)
    setdiff!(tab_fk, tab_pk_fk)
    setdiff!(tab_pk, tab_pk_fk)
    setdiff!(tab_cols, union(tab_fk, tab_pk, tab_pk_fk))
    #
    label_cells_df = DataFrame(
        name=acs[[tab_pk; tab_pk_fk; tab_fk; tab_cols], :col_name],
        type=acs[[tab_pk; tab_pk_fk; tab_fk; tab_cols], :col_type],
        col_guide = label=[fill("pk",length(tab_pk)); fill("pk/fk",length(tab_pk_fk)); fill("fk", length(tab_fk)); fill("data", length(tab_cols))]
    )
    label_cells_df.pk_port = [x.col_guide ∈ ["pk", "pk/fk"] ? """ PORT="pk_$(x.name)" """ : " " for x in eachrow(label_cells_df)]
    label_cells_df.fk_port = [x.col_guide ∈ ["fk", "pk/fk"] ? """ PORT="fk_$(x.name)" """ : " " for x in eachrow(label_cells_df)]
    return label_cells_df
end

"""
Given an acset of schema `SchSqlTables` and a table part ID `tab_id`, generate an HTML-like node label for it.
"""
function make_label_table(acs::T, tab_id) where {T<:AbstractSQLSchema}
    label = String[]
    # name of this table
    tab_name = acs[tab_id, :tab_name]
    # all cols of this table
    tab_cols = get_cols_table(acs, tab_id)
    # make the node header
    push!(label, "$(tab_name) [label=<\n")
    push!(label, """
        <TABLE BORDER="0" CELLSPACING="0" CELLBORDER="1">
            <TR>
                <TD COLSPAN="3" BGCOLOR="#00857C"><FONT COLOR="#FFFFFF" FACE="times-bold">$(tab_name)</FONT></TD>
            </TR>
    """)
    for tr in eachrow(tab_cols)
        push!(label, """
                <TR>
                    <TD$(tr.pk_port)BGCOLOR="#6ECEB2" CELLPADDING="4">$(schema_col_emoji[tr.col_guide])</TD> <TD>$(tr.name)</TD> <TD$(tr.fk_port)>$(tr.type)</TD>
                </TR>
        """)
    end
    # close the table
    push!(label, """
        </TABLE>
    >];
    """)
    return join(label, "")
end

"""
Given an acset of schema `SchSqlTables`, generate all the edges.
"""
function make_edges(acs::T) where {T<:AbstractSQLSchema}
    from_tab = acs[:, (:from, :tab_name)]
    to_tab = acs[:, (:to, :pk_of, :tab_name)]
    from_col = [acs[fk_cols, (:fk_col, :col_name)] for fk_cols in incident(acs, :, :fk)]
    to_col = [acs[pk_cols, (:pk_col, :col_name)] for pk_cols in incident(acs, acs[:, :to], :pk)]
    edges = String[]
    # i indexes over tables
    for i in eachindex(from_tab)
        # j indexes over cols (if a FK goes to a composite PK they have >1 col)
        for j in eachindex(from_col[i])
            push!(edges, "$(from_tab[i]):fk_$(from_col[i][j]):e -> $(to_tab[i]):pk_$(to_col[i][j]):w\n")
        end
    end
    return join(edges, "")
end

"""
Given an acset of schema `SchSqlTables`, generate a string in the Graphviz DOT language
"""
function make_graphviz(acs::T) where {T<:AbstractSQLSchema}
    dot = String[]
    push!(dot, """digraph G {
        graph[rankdir="LR"]
        node[shape="plain"]
    """)
    for t in parts(acs, :Table)
        push!(dot, make_label_table(acs, t))
    end
    push!(dot, make_edges(acs))
    push!(dot, "}")
    return join(dot, "")
end

# make it and visualize
dot_str = make_graphviz(sch_acs)
clipboard(dot_str)