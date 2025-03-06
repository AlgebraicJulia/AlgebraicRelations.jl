using Catlab

# This is a labeled graph whose edges are also labeled
@present SchEdgeLabeledGraph <: SchLabeledGraph begin
    Value::AttrType
    value::Attr(V, Value)
    EdgeLabel::AttrType
    edgelabel::Attr(E, EdgeLabel)
end
@acset_type EdgeLabeledGraph(SchEdgeLabeledGraph)

