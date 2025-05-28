module SchemasRefactor

using Catlab

to_graphviz(AlgebraicRelations.Schemas.TheorySQLSchema, graph_attrs=Dict(:size=>"4.5",:ratio=>"expand"))
to_graphviz(AlgebraicRelations.Fabric.SchERD, graph_attrs=Dict(:size=>"4.5",:ratio=>"expand"))

"""
A schema to describe SQL schemas.
`Table` is what you would expect. `Column` are the columns of a table, so they always know who they belong to col_of.
PK is the primary key of a table pk_of. It needs a junction table PK_Cols because composite primary keys need
to reference multiple columns of a table. FK are foreign keys, they take you from one table to a PK (of another table).
Because the referenced primary key may be composite, the foreign key may also need to be composite, so there is a junction
table FK_Cols to relate a foreign key to its columns.

There are 2 commuting squares. The first is defined by pk_col; col_of == pk; pk_of, saying that a primary key's columns
must belong to the table that it is a primary key for. The second is analogous for foreign keys and is defined by fk; from == fk_col; col_of.
We would also like to say that from != to; pk_of so that foreign keys have to go between different tables but I do not know
how to enforce that.

We set injective constraints (via `unique_index` when calling `@acset_type`) on the following homs:
  * `pk_of`: each primary key can only be the PK of a single table
"""
@present TheorySQLSchemaRefactor(FreeSchema) begin
  (Table, Column, PK, PK_Cols, FK, FK_Cols)::Ob
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
  compose(pk, pk_of) == compose(pk_col, col_of)
  compose(fk_col, col_of) == compose(fk, from)
  # compose(to, pk_of) != from
end

to_graphviz(TheorySQLSchemaRefactor, graph_attrs=Dict(:size=>"4.5",:ratio=>"expand"))

@abstract_acset_type AbstractSQLSchemaRefactor
@acset_type SQLSchemaRefactor(
    TheorySQLSchemaRefactor, 
    index=[:col_of, :pk_col, :pk, :to, :from, :fk, :fk_col], 
    unique_index=[:pk_of]
) <: AbstractSQLSchemaRefactor

@present SchERDRefactor <: TheorySQLSchemaRefactor begin
  Source::Ob
  (Conn, SourceId)::AttrType
  source::Hom(Table, Source)
  source_id::Attr(Source, SourceId)
  conn::Attr(Source, Conn)
end

to_graphviz(SchERDRefactor, graph_attrs=Dict(:size=>"4.5",:ratio=>"expand"))


function to_graphviz_dot(sch::T) where {T<:AbstractSQLSchemaRefactor}
  
end 

end