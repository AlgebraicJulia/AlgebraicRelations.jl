using ACSets
#
using DataFrames
using SQLite
using FunSQL: render

fabric = DataFabric()

conn = SQLite.DB()

d = Database(conn)

add_part!(fabric, :V, label=:sqlite, value=d)

