# TODO change Any to AbstractResult

QueryResultDSGraph = DataSourceGraph{Symbol, Union{DataFrame, Nothing}, Symbol}

struct QueryResultWrapper
    qg::QueryResultDSGraph
    # query
end
export QueryResultWrapper

function QueryResultWrapper(g::DataSourceGraph)
    qg = QueryResultDSGraph()
    add_parts!(qg, :V, nparts(g, :V), label=subpart(g, :label))
    edges = parts(g, :E)
    for e in edges
        foot1 = subpart(g, e, :src)
        foot2 = subpart(g, e, :tgt)
        label1 = subpart(g, foot1, :label)
        label2 = subpart(g, foot2, :label)
        apex = add_part!(qg, :V, label=Symbol("$label1⨝$label2"))
        add_parts!(qg, :E, 2, src=[apex, apex], tgt=[foot1, foot2], edgelabel=[label1, label2])
    end
    QueryResultWrapper(qg)
end
export QueryResultWrapper
