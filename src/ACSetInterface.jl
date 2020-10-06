module ACSetInterface
  export init_tables, prepare, execute, Connection, format_form
  using Catlab
  using ..ACSetDB, ..ACSetQueries
  using LibPQ, DataFrames
  import LibPQ:
    Connection, Result, Statement


  # init_tables:
  # This function will initialize a database with the tables required for the
  # given schema argument

  function init_tables(conn::Connection, schema)
    st = generate_schema_sql(schema)
    result = LibPQ.execute(conn, st)
  end

  # upload_csv:
  # This function uploads data from a CSV to the connected database

  function upload_csv(conn::Connection, table::String, filename::String)
    LibPQ.execute(conn, "COPY '$table' FROM '$filename' DELIMITER ',' CSV HEADER;")
  end

  # prepare:
  # This function creates a prepared statement which can be executed later
  # with variable inserted into the query

  function prepare(conn::Connection, q::Query)::Statement
    uid = LibPQ.unique_id(conn)
    query, n_args = to_prepared_sql(q, uid)
    res = LibPQ.execute(conn, query)
    Statement(conn, uid, query, res, n_args)
  end

  # execute:
  # This function gets the results running the provided query on the connected
  # database

  function execute(conn::Connection, q::Query)::DataFrame
    query = to_sql(q)
    DataFrame(LibPQ.execute(conn, query))
  end

  # execute(Statement, Array):
  # This function runs a prepared SQL statement using the input array as
  # arguments
  function execute(st::Statement, input::AbstractArray)::DataFrame
    DataFrame(LibPQ.execute(st, input))
  end

end
