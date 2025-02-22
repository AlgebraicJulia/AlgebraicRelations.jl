using PEG

macro oursql_str(body, dialect)
    @info body dialect
end

@rule ws = r"\s*"
@rule eq = r"="p
@rule lparen = r"\("
@rule rparen = r"\)"
@rule comma = r","p
@rule EOL = "\n" , ";"
@rule colon = r":"p
@rule identifier = r"[^:{}→\n;=,\(\)\s]+"


oursql"""
    hello
"""mysql
