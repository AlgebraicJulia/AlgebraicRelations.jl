### A Pluto.jl notebook ###
# v0.9.7

using Markdown

# ╔═╡ a16db54a-ab22-11ea-3572-b56df28ffee5
begin
	using Pkg
	Pkg.activate(".")
	using AlgebraicRelations.QueryLib, AlgebraicRelations.SQL, 
		  AlgebraicRelations.Interface;
	using Markdown
end

# ╔═╡ 6b8a17f6-ab23-11ea-3dbb-ff81e614f3cb
begin
	# Define Types
	full_name = Ob(FreeBicategoryRelations, :full_name);
	person = Ob(FreeBicategoryRelations, :person);
	F = Ob(FreeBicategoryRelations, :F);
	ID = Ob(FreeBicategoryRelations, :ID);

	# Define Tables
	names = Hom(:names, person, full_name);
	employees = Hom(:employees, person, ID);
	customers = Hom(:customers, person, ID);
	manager = Hom(:manager, person, person);
	salary = Hom(:salary, person, F);
	relation = Hom(:relation, person⊗person, F);
end

# ╔═╡ a4a2fba2-ab23-11ea-05f9-e5050dfd8e51
begin
	# Data Types
	types  = Dict(:full_name => (["first", "last"],[String,String]),
				  :person    => ([], [Int]),
				  :F         => ([], [Float64]),
				  :ID        => ([], [Int]))

	# Tables -> Column names
	tables = Dict(:names     => (["person"], ["full_name"]),
				  :employees => (["person"],["ID"]),
				  :customers => (["person"],["ID"]),
				  :manager   => (["person"],["manager"]),
				  :salary    => (["person"],["salary"]),
				  :relation  => (["person1", "person2"], ["relationship"]))
end

# ╔═╡ bb93d104-ab23-11ea-35c3-7761d6c2aa51
begin
	formula = Δ(person)⋅((Δ(person)⋅(salary⊗names))⊗(Δ(person)⋅(manager⊗id(person))⋅relation))
	qf = Query(types, tables, formula)
	draw_query(qf)
end

# ╔═╡ 26e14658-ab26-11ea-2dca-e96252dae0b6
begin
	syntax_types  = [full_name, person, F, ID]
	syntax_tables = [names, employees, customers, manager, salary, relation]
	schema = to_presentation(syntax_types, syntax_tables)

	f = @program schema (p::person) begin
	  m = manager(p)
	  return salary(p), names(p), relation(m, p)
	end
	qp = Query(types, tables, f)
	draw_query(qp)
end

# ╔═╡ 66eb6ce2-ab26-11ea-17b5-1f93253e34f1
begin
	conn = Connection("dbname=test_db");
	# Uncomment this to initialize the tables in your database
	# init_tables(conn, types, tables, schema)
	statement = prepare(conn,qp)
	execute(statement, [3])
	close(conn)
end

# ╔═╡ Cell order:
# ╠═a16db54a-ab22-11ea-3572-b56df28ffee5
# ╠═6b8a17f6-ab23-11ea-3dbb-ff81e614f3cb
# ╠═a4a2fba2-ab23-11ea-05f9-e5050dfd8e51
# ╠═bb93d104-ab23-11ea-35c3-7761d6c2aa51
# ╠═26e14658-ab26-11ea-2dca-e96252dae0b6
# ╠═66eb6ce2-ab26-11ea-17b5-1f93253e34f1
