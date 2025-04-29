module SchemasRefactor

using Catlab

"""
A schema to describe SQL schemas.
Tables are what you would expect. Cols are the columns of a table, so they always know who they belong to col_of.
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
@present TheorySQLSchema(FreeSchema) begin
    (Tables,Cols,PK,PK_Cols,FK,FK_Cols)::Ob
    col_of::Hom(Cols,Tables)
    pk_col::Hom(PK_Cols,Cols)
    pk::Hom(PK_Cols,PK)
    pk_of::Hom(PK,Tables)
    to::Hom(FK,PK)
    from::Hom(FK,Tables)
    fk::Hom(FK_Cols,FK)
    fk_col::Hom(FK_Cols,Cols)
    StrType::AttrType
    tab_name::Attr(Tables,StrType)
    col_name::Attr(Cols,StrType)
    col_type::Attr(Cols,StrType)
    compose(pk, pk_of) == compose(pk_col, col_of)
    compose(fk_col, col_of) == compose(fk, from)
    # compose(to, pk_of) != from
end

@abstract_acset_type AbstractSQLSchema
@acset_type SQLSchema(
    TheorySQLSchema, 
    index=[:col_of, :pk_col, :pk, :to, :from, :fk, :fk_col], 
    unique_index=[:pk_of]
) <: AbstractSQLSchema


end