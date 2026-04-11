using ACSets
using Catlab
using AlgebraicRelations

using SQLite, DBInterface
using FunSQL

fabric = DataFabric()

# ─── Department ───
@present SchDepartment(FreeSchema) begin
    Name::AttrType
    Department::Ob
    dept_name::Attr(Department, Name)
end
@acset_type DepartmentT(SchDepartment)
department = InMemory(DepartmentT{Symbol}())
department_src = add_source!(fabric, department)

add_part!(fabric, :Department, dept_name=:Mathematics)      # 1
add_part!(fabric, :Department, dept_name=:Physics)           # 2
add_part!(fabric, :Department, dept_name=:ComputerScience)   # 3
add_part!(fabric, :Department, dept_name=:Biology)           # 4
add_part!(fabric, :Department, dept_name=:Chemistry)         # 5
add_part!(fabric, :Department, dept_name=:Statistics)         # 6
add_part!(fabric, :Department, dept_name=:Engineering)        # 7
add_part!(fabric, :Department, dept_name=:Philosophy)         # 8

# ─── Researcher ───
@present SchResearcher(FreeSchema) begin
    Name::AttrType
    Researcher::Ob
    researcher_name::Attr(Researcher, Name)
    seniority::Attr(Researcher, Name)
end
@acset_type ResearcherT(SchResearcher)
researcher = InMemory(ResearcherT{Symbol}())
researcher_src = add_source!(fabric, researcher)

add_part!(fabric, :Researcher, researcher_name=:Alice,  seniority=:Senior)  # 1
add_part!(fabric, :Researcher, researcher_name=:Bob,    seniority=:Junior)  # 2
add_part!(fabric, :Researcher, researcher_name=:Carol,  seniority=:Senior)  # 3
add_part!(fabric, :Researcher, researcher_name=:Dan,    seniority=:Junior)  # 4
add_part!(fabric, :Researcher, researcher_name=:Eve,    seniority=:Senior)  # 5
add_part!(fabric, :Researcher, researcher_name=:Frank,  seniority=:Junior)  # 6
add_part!(fabric, :Researcher, researcher_name=:Grace,  seniority=:Senior)  # 7
add_part!(fabric, :Researcher, researcher_name=:Heidi,  seniority=:Junior)  # 8
add_part!(fabric, :Researcher, researcher_name=:Ivan,   seniority=:Senior)  # 9
add_part!(fabric, :Researcher, researcher_name=:Judy,   seniority=:Junior)  # 10
add_part!(fabric, :Researcher, researcher_name=:Karl,   seniority=:Senior)  # 11
add_part!(fabric, :Researcher, researcher_name=:Leo,    seniority=:Junior)  # 12
add_part!(fabric, :Researcher, researcher_name=:Mia,    seniority=:Senior)  # 13
add_part!(fabric, :Researcher, researcher_name=:Nate,   seniority=:Junior)  # 14
add_part!(fabric, :Researcher, researcher_name=:Olivia, seniority=:Senior)  # 15
add_part!(fabric, :Researcher, researcher_name=:Pat,    seniority=:Junior)  # 16
add_part!(fabric, :Researcher, researcher_name=:Quinn,  seniority=:Senior)  # 17
add_part!(fabric, :Researcher, researcher_name=:Ruth,   seniority=:Junior)  # 18
add_part!(fabric, :Researcher, researcher_name=:Sam,    seniority=:Senior)  # 19
add_part!(fabric, :Researcher, researcher_name=:Tina,   seniority=:Junior)  # 20

# ─── Paper ───
@present SchPaper(FreeSchema) begin
    Name::AttrType
    Paper::Ob
    title::Attr(Paper, Name)
    year::Attr(Paper, Name)
end
@acset_type PaperT(SchPaper)
paper = InMemory(PaperT{Symbol}())
paper_src = add_source!(fabric, paper)

add_part!(fabric, :Paper, title=:TopologicalDataAnalysis, year=Symbol("2023"))  # 1
add_part!(fabric, :Paper, title=:QuantumEntanglement,     year=Symbol("2022"))  # 2
add_part!(fabric, :Paper, title=:DeepLearningProofs,      year=Symbol("2023"))  # 3
add_part!(fabric, :Paper, title=:ProteinFolding,          year=Symbol("2022"))  # 4
add_part!(fabric, :Paper, title=:CatalyticReactions,      year=Symbol("2021"))  # 5
add_part!(fabric, :Paper, title=:BayesianNetworks,        year=Symbol("2023"))  # 6
add_part!(fabric, :Paper, title=:RobotLocomotion,         year=Symbol("2022"))  # 7
add_part!(fabric, :Paper, title=:ModalLogic,              year=Symbol("2021"))  # 8
add_part!(fabric, :Paper, title=:HomologicalAlgebra,      year=Symbol("2023"))  # 9
add_part!(fabric, :Paper, title=:NeuralODEs,              year=Symbol("2022"))  # 10
add_part!(fabric, :Paper, title=:GenomeAssembly,          year=Symbol("2023"))  # 11
add_part!(fabric, :Paper, title=:MaterialScience,         year=Symbol("2021"))  # 12
add_part!(fabric, :Paper, title=:CausalInference,         year=Symbol("2023"))  # 13
add_part!(fabric, :Paper, title=:FluidDynamics,           year=Symbol("2022"))  # 14
add_part!(fabric, :Paper, title=:CategoryTheory,          year=Symbol("2023"))  # 15

# ─── Journal ───
@present SchJournal(FreeSchema) begin
    Name::AttrType
    Journal::Ob
    journal_name::Attr(Journal, Name)
    field::Attr(Journal, Name)
end
@acset_type JournalT(SchJournal)
journal = InMemory(JournalT{Symbol}())
journal_src = add_source!(fabric, journal)

add_part!(fabric, :Journal, journal_name=:NatureMethods,  field=:Biology)          # 1
add_part!(fabric, :Journal, journal_name=:PhysRevLetters, field=:Physics)          # 2
add_part!(fabric, :Journal, journal_name=:JMLR,           field=:MachineLearning)  # 3
add_part!(fabric, :Journal, journal_name=:AnnalsMath,     field=:Mathematics)      # 4
add_part!(fabric, :Journal, journal_name=:JACS,           field=:Chemistry)        # 5
add_part!(fabric, :Journal, journal_name=:PhilReview,     field=:Philosophy)       # 6

# ─── Grant ───
@present SchGrant(FreeSchema) begin
    Name::AttrType
    Grant::Ob
    grant_name::Attr(Grant, Name)
    agency::Attr(Grant, Name)
end
@acset_type GrantT(SchGrant)
grant = InMemory(GrantT{Symbol}())
grant_src = add_source!(fabric, grant)

add_part!(fabric, :Grant, grant_name=:NSF_DMS_001, agency=:NSF)    # 1
add_part!(fabric, :Grant, grant_name=:NIH_R01_002, agency=:NIH)    # 2
add_part!(fabric, :Grant, grant_name=:DOE_003,     agency=:DOE)    # 3
add_part!(fabric, :Grant, grant_name=:DARPA_004,   agency=:DARPA)  # 4
add_part!(fabric, :Grant, grant_name=:NSF_IIS_005, agency=:NSF)    # 5

# ─── Keyword ───
@present SchKeyword(FreeSchema) begin
    Name::AttrType
    Keyword::Ob
    keyword::Attr(Keyword, Name)
end
@acset_type KeywordT(SchKeyword)
keyword = InMemory(KeywordT{Symbol}())
keyword_src = add_source!(fabric, keyword)

add_part!(fabric, :Keyword, keyword=:topology)   # 1
add_part!(fabric, :Keyword, keyword=:quantum)    # 2
add_part!(fabric, :Keyword, keyword=:learning)   # 3
add_part!(fabric, :Keyword, keyword=:protein)    # 4
add_part!(fabric, :Keyword, keyword=:catalysis)  # 5
add_part!(fabric, :Keyword, keyword=:bayesian)   # 6
add_part!(fabric, :Keyword, keyword=:robotics)   # 7
add_part!(fabric, :Keyword, keyword=:logic)      # 8
add_part!(fabric, :Keyword, keyword=:algebra)    # 9
add_part!(fabric, :Keyword, keyword=:causal)     # 10

# ─── ResearcherDept (junction) ───
@present SchResearcherDept(FreeSchema) begin
    (Researcher, Department)::AttrType
    ResearcherDept::Ob
    rd_researcher::Attr(ResearcherDept, Researcher)
    rd_dept::Attr(ResearcherDept, Department)
end
@acset_type ResearcherDeptT(SchResearcherDept)
researcher_dept = InMemory(ResearcherDeptT{FK{ResearcherT}, FK{DepartmentT}}())
researcher_dept_src = add_source!(fabric, researcher_dept)
add_fk!(fabric, researcher_dept_src, researcher_src, :ResearcherDept!rd_researcher => :Researcher!Researcher_id)
add_fk!(fabric, researcher_dept_src, department_src, :ResearcherDept!rd_dept => :Department!Department_id)

add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(1),  rd_dept=FK{DepartmentT}(1))  # Alice → Math
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(1),  rd_dept=FK{DepartmentT}(6))  # Alice → Stats
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(2),  rd_dept=FK{DepartmentT}(3))  # Bob → CS
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(3),  rd_dept=FK{DepartmentT}(2))  # Carol → Physics
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(3),  rd_dept=FK{DepartmentT}(1))  # Carol → Math
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(4),  rd_dept=FK{DepartmentT}(4))  # Dan → Biology
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(5),  rd_dept=FK{DepartmentT}(3))  # Eve → CS
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(5),  rd_dept=FK{DepartmentT}(6))  # Eve → Stats
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(6),  rd_dept=FK{DepartmentT}(5))  # Frank → Chemistry
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(7),  rd_dept=FK{DepartmentT}(7))  # Grace → Engineering
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(8),  rd_dept=FK{DepartmentT}(4))  # Heidi → Biology
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(8),  rd_dept=FK{DepartmentT}(5))  # Heidi → Chemistry
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(9),  rd_dept=FK{DepartmentT}(1))  # Ivan → Math
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(10), rd_dept=FK{DepartmentT}(3))  # Judy → CS
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(11), rd_dept=FK{DepartmentT}(2))  # Karl → Physics
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(12), rd_dept=FK{DepartmentT}(7))  # Leo → Engineering
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(12), rd_dept=FK{DepartmentT}(3))  # Leo → CS
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(13), rd_dept=FK{DepartmentT}(6))  # Mia → Stats
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(13), rd_dept=FK{DepartmentT}(1))  # Mia → Math
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(14), rd_dept=FK{DepartmentT}(4))  # Nate → Biology
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(15), rd_dept=FK{DepartmentT}(8))  # Olivia → Philosophy
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(16), rd_dept=FK{DepartmentT}(2))  # Pat → Physics
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(17), rd_dept=FK{DepartmentT}(1))  # Quinn → Math
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(17), rd_dept=FK{DepartmentT}(8))  # Quinn → Philosophy
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(18), rd_dept=FK{DepartmentT}(5))  # Ruth → Chemistry
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(19), rd_dept=FK{DepartmentT}(7))  # Sam → Engineering
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(19), rd_dept=FK{DepartmentT}(2))  # Sam → Physics
add_part!(fabric, :ResearcherDept, rd_researcher=FK{ResearcherT}(20), rd_dept=FK{DepartmentT}(3))  # Tina → CS

# ─── PaperAuthor (junction) ───
@present SchPaperAuthor(FreeSchema) begin
    (Paper, Researcher)::AttrType
    PaperAuthor::Ob
    pa_paper::Attr(PaperAuthor, Paper)
    pa_researcher::Attr(PaperAuthor, Researcher)
end
@acset_type PaperAuthorT(SchPaperAuthor)
paper_author = InMemory(PaperAuthorT{FK{PaperT}, FK{ResearcherT}}())
paper_author_src = add_source!(fabric, paper_author)
add_fk!(fabric, paper_author_src, paper_src, :PaperAuthor!pa_paper => :Paper!Paper_id)
add_fk!(fabric, paper_author_src, researcher_src, :PaperAuthor!pa_researcher => :Researcher!Researcher_id)

add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(1),  pa_researcher=FK{ResearcherT}(1))   # TDA: Alice
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(1),  pa_researcher=FK{ResearcherT}(9))   # TDA: Ivan
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(2),  pa_researcher=FK{ResearcherT}(3))   # QuantumEntanglement: Carol
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(2),  pa_researcher=FK{ResearcherT}(11))  # QuantumEntanglement: Karl
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(3),  pa_researcher=FK{ResearcherT}(2))   # DeepLearningProofs: Bob
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(3),  pa_researcher=FK{ResearcherT}(5))   # DeepLearningProofs: Eve
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(4),  pa_researcher=FK{ResearcherT}(4))   # ProteinFolding: Dan
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(4),  pa_researcher=FK{ResearcherT}(8))   # ProteinFolding: Heidi
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(5),  pa_researcher=FK{ResearcherT}(6))   # CatalyticReactions: Frank
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(5),  pa_researcher=FK{ResearcherT}(18))  # CatalyticReactions: Ruth
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(6),  pa_researcher=FK{ResearcherT}(5))   # BayesianNetworks: Eve
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(6),  pa_researcher=FK{ResearcherT}(13))  # BayesianNetworks: Mia
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(7),  pa_researcher=FK{ResearcherT}(7))   # RobotLocomotion: Grace
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(7),  pa_researcher=FK{ResearcherT}(12))  # RobotLocomotion: Leo
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(8),  pa_researcher=FK{ResearcherT}(15))  # ModalLogic: Olivia
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(8),  pa_researcher=FK{ResearcherT}(17))  # ModalLogic: Quinn
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(9),  pa_researcher=FK{ResearcherT}(1))   # HomologicalAlgebra: Alice
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(9),  pa_researcher=FK{ResearcherT}(17))  # HomologicalAlgebra: Quinn
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(10), pa_researcher=FK{ResearcherT}(2))   # NeuralODEs: Bob
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(10), pa_researcher=FK{ResearcherT}(10))  # NeuralODEs: Judy
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(11), pa_researcher=FK{ResearcherT}(4))   # GenomeAssembly: Dan
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(11), pa_researcher=FK{ResearcherT}(14))  # GenomeAssembly: Nate
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(12), pa_researcher=FK{ResearcherT}(16))  # MaterialScience: Pat
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(12), pa_researcher=FK{ResearcherT}(19))  # MaterialScience: Sam
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(13), pa_researcher=FK{ResearcherT}(5))   # CausalInference: Eve
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(13), pa_researcher=FK{ResearcherT}(13))  # CausalInference: Mia
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(14), pa_researcher=FK{ResearcherT}(11))  # FluidDynamics: Karl
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(14), pa_researcher=FK{ResearcherT}(19))  # FluidDynamics: Sam
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(15), pa_researcher=FK{ResearcherT}(9))   # CategoryTheory: Ivan
add_part!(fabric, :PaperAuthor, pa_paper=FK{PaperT}(15), pa_researcher=FK{ResearcherT}(17))  # CategoryTheory: Quinn

# ─── PaperJournal (junction) ───
@present SchPaperJournal(FreeSchema) begin
    (Paper, Journal)::AttrType
    PaperJournal::Ob
    pj_paper::Attr(PaperJournal, Paper)
    pj_journal::Attr(PaperJournal, Journal)
end
@acset_type PaperJournalT(SchPaperJournal)
paper_journal = InMemory(PaperJournalT{FK{PaperT}, FK{JournalT}}())
paper_journal_src = add_source!(fabric, paper_journal)
add_fk!(fabric, paper_journal_src, paper_src, :PaperJournal!pj_paper => :Paper!Paper_id)
add_fk!(fabric, paper_journal_src, journal_src, :PaperJournal!pj_journal => :Journal!Journal_id)

add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(1),  pj_journal=FK{JournalT}(4))  # TDA → AnnalsMath
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(2),  pj_journal=FK{JournalT}(2))  # QuantumEntanglement → PhysRevLetters
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(3),  pj_journal=FK{JournalT}(3))  # DeepLearningProofs → JMLR
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(4),  pj_journal=FK{JournalT}(1))  # ProteinFolding → NatureMethods
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(5),  pj_journal=FK{JournalT}(5))  # CatalyticReactions → JACS
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(6),  pj_journal=FK{JournalT}(3))  # BayesianNetworks → JMLR
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(7),  pj_journal=FK{JournalT}(3))  # RobotLocomotion → JMLR
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(8),  pj_journal=FK{JournalT}(6))  # ModalLogic → PhilReview
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(9),  pj_journal=FK{JournalT}(4))  # HomologicalAlgebra → AnnalsMath
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(10), pj_journal=FK{JournalT}(3))  # NeuralODEs → JMLR
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(11), pj_journal=FK{JournalT}(1))  # GenomeAssembly → NatureMethods
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(12), pj_journal=FK{JournalT}(5))  # MaterialScience → JACS
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(13), pj_journal=FK{JournalT}(3))  # CausalInference → JMLR
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(14), pj_journal=FK{JournalT}(2))  # FluidDynamics → PhysRevLetters
add_part!(fabric, :PaperJournal, pj_paper=FK{PaperT}(15), pj_journal=FK{JournalT}(4))  # CategoryTheory → AnnalsMath

# ─── PaperKeyword (junction) ───
@present SchPaperKeyword(FreeSchema) begin
    (Paper, Keyword)::AttrType
    PaperKeyword::Ob
    pk_paper::Attr(PaperKeyword, Paper)
    pk_keyword::Attr(PaperKeyword, Keyword)
end
@acset_type PaperKeywordT(SchPaperKeyword)
paper_keyword = InMemory(PaperKeywordT{FK{PaperT}, FK{KeywordT}}())
paper_keyword_src = add_source!(fabric, paper_keyword)
add_fk!(fabric, paper_keyword_src, paper_src, :PaperKeyword!pk_paper => :Paper!Paper_id)
add_fk!(fabric, paper_keyword_src, keyword_src, :PaperKeyword!pk_keyword => :Keyword!Keyword_id)

add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(1),  pk_keyword=FK{KeywordT}(1))   # TDA: topology
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(1),  pk_keyword=FK{KeywordT}(9))   # TDA: algebra
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(2),  pk_keyword=FK{KeywordT}(2))   # QuantumEntanglement: quantum
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(3),  pk_keyword=FK{KeywordT}(3))   # DeepLearningProofs: learning
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(3),  pk_keyword=FK{KeywordT}(9))   # DeepLearningProofs: algebra
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(4),  pk_keyword=FK{KeywordT}(4))   # ProteinFolding: protein
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(5),  pk_keyword=FK{KeywordT}(5))   # CatalyticReactions: catalysis
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(6),  pk_keyword=FK{KeywordT}(6))   # BayesianNetworks: bayesian
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(6),  pk_keyword=FK{KeywordT}(10))  # BayesianNetworks: causal
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(7),  pk_keyword=FK{KeywordT}(7))   # RobotLocomotion: robotics
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(8),  pk_keyword=FK{KeywordT}(8))   # ModalLogic: logic
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(9),  pk_keyword=FK{KeywordT}(9))   # HomologicalAlgebra: algebra
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(10), pk_keyword=FK{KeywordT}(3))   # NeuralODEs: learning
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(11), pk_keyword=FK{KeywordT}(4))   # GenomeAssembly: protein
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(12), pk_keyword=FK{KeywordT}(5))   # MaterialScience: catalysis
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(13), pk_keyword=FK{KeywordT}(10))  # CausalInference: causal
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(13), pk_keyword=FK{KeywordT}(6))   # CausalInference: bayesian
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(14), pk_keyword=FK{KeywordT}(2))   # FluidDynamics: quantum
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(15), pk_keyword=FK{KeywordT}(9))   # CategoryTheory: algebra
add_part!(fabric, :PaperKeyword, pk_paper=FK{PaperT}(15), pk_keyword=FK{KeywordT}(8))   # CategoryTheory: logic

# ─── GrantDept (junction) ───
@present SchGrantDept(FreeSchema) begin
    (Grant, Department)::AttrType
    GrantDept::Ob
    gd_grant::Attr(GrantDept, Grant)
    gd_dept::Attr(GrantDept, Department)
end
@acset_type GrantDeptT(SchGrantDept)
grant_dept = InMemory(GrantDeptT{FK{GrantT}, FK{DepartmentT}}())
grant_dept_src = add_source!(fabric, grant_dept)
add_fk!(fabric, grant_dept_src, grant_src, :GrantDept!gd_grant => :Grant!Grant_id)
add_fk!(fabric, grant_dept_src, department_src, :GrantDept!gd_dept => :Department!Department_id)

add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(1), gd_dept=FK{DepartmentT}(1))  # NSF_DMS → Math
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(1), gd_dept=FK{DepartmentT}(6))  # NSF_DMS → Stats
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(2), gd_dept=FK{DepartmentT}(4))  # NIH_R01 → Biology
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(2), gd_dept=FK{DepartmentT}(5))  # NIH_R01 → Chemistry
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(3), gd_dept=FK{DepartmentT}(2))  # DOE → Physics
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(3), gd_dept=FK{DepartmentT}(7))  # DOE → Engineering
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(4), gd_dept=FK{DepartmentT}(3))  # DARPA → CS
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(4), gd_dept=FK{DepartmentT}(7))  # DARPA → Engineering
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(5), gd_dept=FK{DepartmentT}(3))  # NSF_IIS → CS
add_part!(fabric, :GrantDept, gd_grant=FK{GrantT}(5), gd_dept=FK{DepartmentT}(6))  # NSF_IIS → Stats

# ─── Queries ───

# Query 1 (simple join): "What papers did each researcher author?"
q1 = @relation (rname=rn, ptitle=pt) begin
    PaperAuthor(pa_researcher=r, pa_paper=p)
    Researcher(id=r, researcher_name=rn)
    Paper(id=p, title=pt)
end

# Query 2 (chain): "Which researchers published where, from which department?"
# Filter after for dept_name == :Mathematics and journal_name == :AnnalsMath
# Expected matches: Alice, Ivan, Quinn
q2 = @relation (rname=rn, dname=dn, jname=jn) begin
    ResearcherDept(rd_researcher=r, rd_dept=d)
    Department(id=d, dept_name=dn)
    PaperAuthor(pa_researcher=r, pa_paper=p)
    PaperJournal(pj_paper=p, pj_journal=j)
    Journal(id=j, journal_name=jn)
    Researcher(id=r, researcher_name=rn)
end

# Query 3 (cyclic): "Find co-authors who share a department"
# Cycle: r1 → paper ← r2, r1 → dept ← r2
# Includes self-pairs; filter r1 != r2 after
q3 = @relation (r1name=r1n, r2name=r2n, dname=dn) begin
    PaperAuthor(pa_paper=p, pa_researcher=r1)
    PaperAuthor(pa_paper=p, pa_researcher=r2)
    ResearcherDept(rd_researcher=r1, rd_dept=d)
    ResearcherDept(rd_researcher=r2, rd_dept=d)
    Researcher(id=r1, researcher_name=r1n)
    Researcher(id=r2, researcher_name=r2n)
    Department(id=d, dept_name=dn)
end

# Query 4 (star): "For each grant agency, which keywords appear in
#   papers by researchers in funded departments?"
q4 = @relation (ag=ag, kw=kw) begin
    GrantDept(gd_grant=g, gd_dept=d)
    Grant(id=g, agency=ag)
    ResearcherDept(rd_dept=d, rd_researcher=r)
    PaperAuthor(pa_researcher=r, pa_paper=p)
    PaperKeyword(pk_paper=p, pk_keyword=k)
    Keyword(id=k, keyword=kw)
end

# Query 5 (complex, 9 boxes): "Full path from grant to journal field and keyword"
q5 = @relation (field=jf, keyword=kw, agency=ag, dept_name=dn) begin
    GrantDept(gd_grant=g, gd_dept=d)
    Grant(id=g, agency=ag)
    Department(id=d, dept_name=dn)
    ResearcherDept(rd_dept=d, rd_researcher=r)
    PaperAuthor(pa_researcher=r, pa_paper=p)
    PaperJournal(pj_paper=p, pj_journal=j)
    Journal(id=j, field=jf)
    PaperKeyword(pk_paper=p, pk_keyword=k)
    Keyword(id=k, keyword=kw)
end

q = prepare(q5, fabric)

using DataFrames

df=DataFrame(q)

q5_filtered = @relation (jfield=jf, kw=kw) begin
    GrantDept(gd_grant=g, gd_dept=d)
    Grant(id=g, agency=agency)
    Department(id=d, dept_name=dept_name)
    ResearcherDept(rd_dept=d, rd_researcher=r)
    PaperAuthor(pa_researcher=r, pa_paper=p)
    PaperJournal(pj_paper=p, pj_journal=j)
    Journal(id=j, field=jf)
    PaperKeyword(pk_paper=p, pk_keyword=k)
    Keyword(id=k, keyword=kw)
    AgencyFilter(agency=agency)
    DeptFilter(dept_name=dept_name)
end

q_filtered = prepare(q5_filtered, fabric, filters=Dict(
    :agency => :NSF,
    :dept_name => :Mathematics,
))

df=DataFrame(q)
