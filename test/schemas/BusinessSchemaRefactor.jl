# changed the busines schema so Salary has PK "Grade" otherwise if 
# it's just a float it doesn't really make sense to have Salary and
# the junction table Income...we'd just have another Column "salary" for Employee!

using SQLite, DBInterface
using DataFrames

db = DBInterface.connect(SQLite.DB, ":memory:")

# DBInterface can't execute multiple queries in one go, and can't read in a .sql file
# with mtple queries either
DBInterface.execute(db, """
    CREATE TABLE Employee (
        name TEXT PRIMARY KEY
    );
""")

DBInterface.execute(db, """
    CREATE TABLE Manager (
        employee REFERENCES Employee(name),
        manager REFERENCES Employee(name),
        PRIMARY KEY (employee, manager)
    );
""")

DBInterface.execute(db, """
    CREATE TABLE Income (
        employee REFERENCES Employee(name),
        salary REFERENCES Salary(grade),
        PRIMARY KEY (employee, salary)
    );
""")

DBInterface.execute(db, """
    CREATE TABLE Salary (
        grade INTEGER PRIMARY KEY,
        salary REAL
    );
""")


DBInterface.execute(db, "SELECT * FROM sqlite_schema;") |> DataFrame
DBInterface.execute(db, "PRAGMA index_list(Manager);") |> DataFrame
DBInterface.execute(db, "PRAGMA index_info(sqlite_autoindex_Manager_1);") |> DataFrame
DBInterface.execute(db, "PRAGMA table_info(Manager);") |> DataFrame
DBInterface.execute(db, "PRAGMA table_info(Salary);") |> DataFrame
DBInterface.execute(db, "PRAGMA foreign_key_list(Income);") |> DataFrame
DBInterface.execute(db, "PRAGMA foreign_key_list(Manager);") |> DataFrame

