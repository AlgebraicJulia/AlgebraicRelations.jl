module Interface
  export init_tables, prepare, execute, Connection, format_form 

  using Catlab
  using AlgebraicRelations.Presentation, AlgebraicRelations.QueryLib, AlgebraicRelations.SQL
  using LibPQ, DataFrames
  import LibPQ:
    Connection, Result, Statement


  function init_tables(conn::Connection, schema::Schema)
    prim, tab = sql(schema)
  end

  # prepare:
  # This function creates a prepared statement which can be executed later
  # with variable inserted into the query

  function prepare(conn::Connection, schema::Schema, expr::GATExpr)::Statement
    query = make_query(schema, expr)
    
    # Need to generate a wrapper call around this to insert parameters
    types = Array{String,1}()
    uid = string(rand(1:1000000))
    pre   = "SELECT * FROM\n("
    sym_count = 1
    post  = ")\n AS A WHERE " * join(map(enumerate(query.dom_names)) do (i,a)
                                       val= ""
                                       if length(query.dom.sub_fields[i]) > 0
                                         val = "ROW("
                                         for j in 1:length(query.dom.sub_fields[i])
                                           if j != 1
                                             val *= ","
                                           end
                                           val *= "\$$sym_count"
                                           sym_count += 1
                                           append!(types, [query.dom.sub_fields[i][j]])
                                         end
                                         val *= ")"
                                       else
                                         val = "\$$sym_count"
                                         sym_count += 1
                                         append!(types, [query.dom.types[i]])
                                       end
                                       "$a=$val"
                                     end, " AND ")
    type_str = " (" * join(types,",") * ") AS "
    res = LibPQ.execute(conn, "PREPARE "* "\"$uid\"" * type_str * pre * query.query * post)
    Statement(conn, uid, query.query, res, length(types))
  end

  function execute(conn::Connection, schema::Schema, expr::GATExpr)::DataFrame
    query = make_query(schema, expr)
    DataFrame(LibPQ.execute(conn, query.query))
  end

  function execute(st::Statement, input::AbstractArray)::DataFrame
    DataFrame(LibPQ.execute(st, input))
  end

  function to_diagram(formula)
    temp_form = deepcopy(formula)
    format_form(temp_form)
  end

  function format_form!(formula)
    if typeof(formula).parameters[1] == :generator
      if hasproperty(formula.args[1], :name)
        formula.args[1] = formula.args[1].name
      end
    else
      for val in formula.args
        format_form(val)
      end
    end
  end

end
