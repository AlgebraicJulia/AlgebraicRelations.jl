module Interface
  export init_tables, prepare, execute, Connection, format_form
  using Catlab
  using AlgebraicRelations.SchemaLib, AlgebraicRelations.QueryLib, AlgebraicRelations.SQL
  using LibPQ, DataFrames
  import LibPQ:
    Connection, Result, Statement


  function init_tables(conn::Connection, types, tables, schema)
    st = sql(types, tables, schema)
    result = LibPQ.execute(conn, st)
  end

  function upload_csv(conn::Connection, table::String, filename::String)
    LibPQ.execute(conn, "COPY '$table' FROM '$filename' DELIMITER ',' CSV HEADER;")
  end

  # prepare:
  # This function creates a prepared statement which can be executed later
  # with variable inserted into the query

  function prepare(conn::Connection, q::Query)::Statement
    uid = LibPQ.unique_id(conn)
    query = present_sql(q, uid)
    res = LibPQ.execute(conn, query)
    Statement(conn, uid, query, res, length(q.wd.input_ports))
  end

  function execute(conn::Connection, q::Query)::DataFrame
    query = sql(q)
    DataFrame(LibPQ.execute(conn, query))
  end

  function execute(st::Statement, input::AbstractArray)::DataFrame
    DataFrame(LibPQ.execute(st, input))
  end

end
