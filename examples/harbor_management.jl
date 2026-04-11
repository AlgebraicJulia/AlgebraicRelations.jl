using ACSets
using Catlab
using AlgebraicRelations
using SQLite, DBInterface
using FunSQL
using Random

Random.seed!(42)

τ = AlgebraicRelations.SQL.DatabaseDS.DBSourceTrait()
fabric = DataFabric()

# ═══════════════════════════════════════════════════════════════════════
# Schema: Harbor Management System
#
# Entity tables:
#   Port, Berth, Vessel, VesselType, Country, CargoType,
#   Pilot, CrewRank, InspectionType, FuelType, Agent
#
# Junction/transaction tables:
#   VesselVisit (Vessel visits a Port, assigned a Berth, Pilot, Agent)
#   CrewMember (crew on a vessel with a rank)
#   CargoManifest (cargo on a vessel visit)
#   Inspection (inspection of a vessel visit)
#   FuelTransaction (fuel delivered to a vessel visit)
#   VesselCountry (vessel registered in country — flag state)
#   PortCountry (port located in country)
#   PilotPort (pilot licensed at port)
#   AgentPort (agent operates at port)
#
# Cyclic query: "Find vessels whose flag state matches the port's country
#   AND whose assigned pilot is licensed at that same port —
#   filtered by country. This finds domestic vessels with local pilots."
#
#   Cycle: country ← VesselCountry → Vessel → VesselVisit → Port → PortCountry → country
#          AND: VesselVisit → Pilot → PilotPort → Port (same port junction)
# ═══════════════════════════════════════════════════════════════════════

# ─── Helper: generate Symbol names ───
sym(prefix, i) = Symbol("$(prefix)_$(lpad(i, 3, '0'))")

# ═══════════════════════════════════════════════════════════════════════
# REFERENCE / ENTITY TABLES (InMemory)
# ═══════════════════════════════════════════════════════════════════════

# ─── Country (InMemory) ───
@present SchCountry(FreeSchema) begin
    Name::AttrType
    Country::Ob
    country_name::Attr(Country, Name)
    country_code::Attr(Country, Name)
end
@acset_type CountryT(SchCountry)
country = InMemory(CountryT{Symbol}())
country_src = add_source!(fabric, country)

country_data = [
    (:UnitedStates, :US), (:UnitedKingdom, :GB), (:Germany, :DE), (:France, :FR),
    (:Netherlands, :NL), (:China, :CN), (:Japan, :JP), (:SouthKorea, :KR),
    (:Singapore, :SG), (:Panama, :PA), (:Liberia, :LR), (:MarshallIslands, :MH),
    (:Norway, :NO), (:Greece, :GR), (:Italy, :IT), (:Spain, :ES),
    (:Brazil, :BR), (:India, :IN), (:Australia, :AU), (:Canada, :CA),
]
for (name, code) in country_data
    add_part!(fabric, :Country, country_name=name, country_code=code)
end

# ─── VesselType (InMemory) ───
@present SchVesselType(FreeSchema) begin
    Name::AttrType
    VesselType::Ob
    vtype_name::Attr(VesselType, Name)
end
@acset_type VesselTypeT(SchVesselType)
vessel_type = InMemory(VesselTypeT{Symbol}())
vessel_type_src = add_source!(fabric, vessel_type)

for vt in [:Tanker, :BulkCarrier, :ContainerShip, :RoRo, :LNGCarrier,
           :GeneralCargo, :CruiseShip, :Tugboat, :FishingVessel, :NavalVessel]
    add_part!(fabric, :VesselType, vtype_name=vt)
end

# ─── CargoType (InMemory) ───
@present SchCargoType(FreeSchema) begin
    Name::AttrType
    CargoType::Ob
    cargo_type_name::Attr(CargoType, Name)
    hazardous::Attr(CargoType, Name)
end
@acset_type CargoTypeT(SchCargoType)
cargo_type = InMemory(CargoTypeT{Symbol}())
cargo_type_src = add_source!(fabric, cargo_type)

cargo_data = [
    (:CrudeOil, :yes), (:RefinedPetroleum, :yes), (:LNG, :yes),
    (:IronOre, :no), (:Coal, :no), (:Grain, :no), (:Lumber, :no),
    (:Containers, :no), (:Vehicles, :no), (:Chemicals, :yes),
    (:Fertilizer, :no), (:Steel, :no), (:Cement, :no),
    (:Passengers, :no), (:FrozenGoods, :no),
]
for (name, haz) in cargo_data
    add_part!(fabric, :CargoType, cargo_type_name=name, hazardous=haz)
end

# ─── CrewRank (InMemory) ───
@present SchCrewRank(FreeSchema) begin
    Name::AttrType
    CrewRank::Ob
    rank_name::Attr(CrewRank, Name)
end
@acset_type CrewRankT(SchCrewRank)
crew_rank = InMemory(CrewRankT{Symbol}())
crew_rank_src = add_source!(fabric, crew_rank)

for rk in [:Captain, :FirstOfficer, :ChiefEngineer, :SecondOfficer,
           :Bosun, :ABSeaman, :Oiler, :Cook, :Steward, :Cadet]
    add_part!(fabric, :CrewRank, rank_name=rk)
end

# ─── InspectionType (InMemory) ───
@present SchInspectionType(FreeSchema) begin
    Name::AttrType
    InspectionType::Ob
    insp_type_name::Attr(InspectionType, Name)
end
@acset_type InspectionTypeT(SchInspectionType)
inspection_type = InMemory(InspectionTypeT{Symbol}())
inspection_type_src = add_source!(fabric, inspection_type)

for it in [:PortStateControl, :FlagStateInspection, :ClassSurvey,
           :SafetyInspection, :PollutionPrevention, :SecurityAudit,
           :CargoInspection, :CrewCertification]
    add_part!(fabric, :InspectionType, insp_type_name=it)
end

# ─── FuelType (InMemory) ───
@present SchFuelType(FreeSchema) begin
    Name::AttrType
    FuelType::Ob
    fuel_name::Attr(FuelType, Name)
end
@acset_type FuelTypeT(SchFuelType)
fuel_type = InMemory(FuelTypeT{Symbol}())
fuel_type_src = add_source!(fabric, fuel_type)

for ft in [:HFO, :MGO, :VLSFO, :LNG_Fuel, :Methanol, :Diesel]
    add_part!(fabric, :FuelType, fuel_name=ft)
end

# ─── Port (InMemory) ───
@present SchPort(FreeSchema) begin
    Name::AttrType
    Port::Ob
    port_name::Attr(Port, Name)
    port_code::Attr(Port, Name)
end
@acset_type PortT(SchPort)
port = InMemory(PortT{Symbol}())
port_src = add_source!(fabric, port)

port_data = [
    (:Houston, :USHOU), (:LosAngeles, :USLAX), (:NewYork, :USNYC),
    (:Savannah, :USSAV), (:Seattle, :USSEA), (:Rotterdam, :NLRTM),
    (:Hamburg, :DEHAM), (:Antwerp, :BEANR), (:Shanghai, :CNSHA),
    (:Shenzhen, :CNSZX), (:Singapore, :SGSIN), (:Busan, :KRPUS),
    (:Tokyo, :JPTYO), (:Piraeus, :GRPIR), (:Genoa, :ITGOA),
    (:Barcelona, :ESBCN), (:Santos, :BRSTS), (:Mumbai, :INBOM),
    (:Sydney, :AUSYD), (:Vancouver, :CAVAN), (:Felixstowe, :GBFXT),
    (:Southampton, :GBSOU), (:LeHavre, :FRLEH), (:Marseille, :FRMAR),
    (:Bremerhaven, :DEBRV), (:Yokohama, :JPYOK), (:Kobe, :JPUKB),
    (:Kaohsiung, :TWKHH), (:PortKlang, :MYPKG), (:Durban, :ZADUR),
]
for (name, code) in port_data
    add_part!(fabric, :Port, port_name=name, port_code=code)
end

# ─── PortCountry (InMemory) ───
@present SchPortCountry(FreeSchema) begin
    (Port, Country)::AttrType
    PortCountry::Ob
    ptc_port::Attr(PortCountry, Port)
    ptc_country::Attr(PortCountry, Country)
end
@acset_type PortCountryT(SchPortCountry)
port_country = InMemory(PortCountryT{FK{PortT}, FK{CountryT}}())
port_country_src = add_source!(fabric, port_country)
add_fk!(fabric, port_country_src, port_src, :PortCountry!ptc_port => :Port!Port_id)
add_fk!(fabric, port_country_src, country_src, :PortCountry!ptc_country => :Country!Country_id)

# port_id → country_id mapping
port_country_map = [
    1=>1, 2=>1, 3=>1, 4=>1, 5=>1,  # US ports
    6=>5, 7=>3, 8=>5,               # Rotterdam(NL), Hamburg(DE), Antwerp(NL-ish, use NL)
    9=>6, 10=>6,                     # Shanghai, Shenzhen → China
    11=>9, 12=>8,                    # Singapore, Busan → Korea
    13=>7, 14=>14,                   # Tokyo → Japan, Piraeus → Greece
    15=>15, 16=>16,                  # Genoa → Italy, Barcelona → Spain
    17=>17, 18=>18,                  # Santos → Brazil, Mumbai → India
    19=>19, 20=>20,                  # Sydney → Australia, Vancouver → Canada
    21=>2, 22=>2,                    # Felixstowe, Southampton → UK
    23=>4, 24=>4,                    # LeHavre, Marseille → France
    25=>3,                           # Bremerhaven → Germany
    26=>7, 27=>7,                    # Yokohama, Kobe → Japan
    28=>6, 29=>9, 30=>18,            # Kaohsiung(use CN), PortKlang(use SG), Durban(use India as placeholder)
]
for (p, c) in port_country_map
    add_part!(fabric, :PortCountry, ptc_port=FK{PortT}(p), ptc_country=FK{CountryT}(c))
end

# ─── Berth (InMemory) ───
@present SchBerth(FreeSchema) begin
    (Name, Port)::AttrType
    Berth::Ob
    berth_name::Attr(Berth, Name)
    berth_port::Attr(Berth, Port)
    berth_depth::Attr(Berth, Name)
end
@acset_type BerthT(SchBerth)
berth = InMemory(BerthT{Symbol, FK{PortT}}())
berth_src = add_source!(fabric, berth)
add_fk!(fabric, berth_src, port_src, :Berth!berth_port => :Port!Port_id)

# 5 berths per port = 150 berths
let berth_id = 0
for p in 1:30
    for b in 1:5
        berth_id += 1
        depth = rand([:d10m, :d12m, :d14m, :d16m, :d18m])
        add_part!(fabric, :Berth, berth_name=sym("B", berth_id), berth_port=FK{PortT}(p), berth_depth=depth)
    end
end
end

# ─── Vessel (InMemory) ───
@present SchVessel(FreeSchema) begin
    (Name, VesselType)::AttrType
    Vessel::Ob
    vessel_name::Attr(Vessel, Name)
    imo_number::Attr(Vessel, Name)
    vsl_type::Attr(Vessel, VesselType)
end
@acset_type VesselT(SchVessel)
vessel = InMemory(VesselT{Symbol, FK{VesselTypeT}}())
vessel_src = add_source!(fabric, vessel)
add_fk!(fabric, vessel_src, vessel_type_src, :Vessel!vsl_type => :VesselType!VesselType_id)

# 200 vessels
vessel_names = [sym("VSL", i) for i in 1:200]
for i in 1:200
    vt = rand(1:10)
    add_part!(fabric, :Vessel, vessel_name=vessel_names[i], imo_number=sym("IMO", 9000000+i), vsl_type=FK{VesselTypeT}(vt))
end

# ─── VesselCountry (InMemory) ── flag state registration ───
@present SchVesselCountry(FreeSchema) begin
    (Vessel, Country)::AttrType
    VesselCountry::Ob
    vc_vessel::Attr(VesselCountry, Vessel)
    vc_country::Attr(VesselCountry, Country)
end
@acset_type VesselCountryT(SchVesselCountry)
vessel_country = InMemory(VesselCountryT{FK{VesselT}, FK{CountryT}}())
vessel_country_src = add_source!(fabric, vessel_country)
add_fk!(fabric, vessel_country_src, vessel_src, :VesselCountry!vc_vessel => :Vessel!Vessel_id)
add_fk!(fabric, vessel_country_src, country_src, :VesselCountry!vc_country => :Country!Country_id)

# each vessel registered in 1 country (flag of convenience common: Panama, Liberia, Marshall Islands)
flag_weights = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
for i in 1:200
    # bias toward flag-of-convenience states + major maritime nations
    c = rand([1,1,2,3,5,6,6,7,8,9,10,10,10,11,11,11,12,12,13,14])
    add_part!(fabric, :VesselCountry, vc_vessel=FK{VesselT}(i), vc_country=FK{CountryT}(c))
end

# ─── Pilot (InMemory) ───
@present SchPilot(FreeSchema) begin
    Name::AttrType
    Pilot::Ob
    pilot_name::Attr(Pilot, Name)
    pilot_license::Attr(Pilot, Name)
end
@acset_type PilotT(SchPilot)
pilot = InMemory(PilotT{Symbol}())
pilot_src = add_source!(fabric, pilot)

for i in 1:120
    add_part!(fabric, :Pilot, pilot_name=sym("Pilot", i), pilot_license=sym("LIC", 10000+i))
end

# ─── PilotPort (InMemory) ── pilot licensed at port ───
@present SchPilotPort(FreeSchema) begin
    (Pilot, Port)::AttrType
    PilotPort::Ob
    pp_pilot::Attr(PilotPort, Pilot)
    pp_port::Attr(PilotPort, Port)
end
@acset_type PilotPortT(SchPilotPort)
pilot_port = InMemory(PilotPortT{FK{PilotT}, FK{PortT}}())
pilot_port_src = add_source!(fabric, pilot_port)
add_fk!(fabric, pilot_port_src, pilot_src, :PilotPort!pp_pilot => :Pilot!Pilot_id)
add_fk!(fabric, pilot_port_src, port_src, :PilotPort!pp_port => :Port!Port_id)

# 4 pilots per port, each licensed at 1-2 ports
let pilot_id = 0
for p in 1:30
    for _ in 1:4
        pilot_id += 1
        if pilot_id <= 120
            add_part!(fabric, :PilotPort, pp_pilot=FK{PilotT}(pilot_id), pp_port=FK{PortT}(p))
            # some pilots licensed at neighboring port too
            if rand() < 0.3 && p < 30
                add_part!(fabric, :PilotPort, pp_pilot=FK{PilotT}(pilot_id), pp_port=FK{PortT}(p+1))
            end
        end
    end
end
end

# ─── Agent (InMemory) ───
@present SchAgent(FreeSchema) begin
    Name::AttrType
    Agent::Ob
    agent_name::Attr(Agent, Name)
end
@acset_type AgentT(SchAgent)
agent = InMemory(AgentT{Symbol}())
agent_src = add_source!(fabric, agent)

for i in 1:60
    add_part!(fabric, :Agent, agent_name=sym("Agent", i))
end

# ─── AgentPort (InMemory) ── agent operates at port ───
@present SchAgentPort(FreeSchema) begin
    (Agent, Port)::AttrType
    AgentPort::Ob
    ap_agent::Attr(AgentPort, Agent)
    ap_port::Attr(AgentPort, Port)
end
@acset_type AgentPortT(SchAgentPort)
agent_port = InMemory(AgentPortT{FK{AgentT}, FK{PortT}}())
agent_port_src = add_source!(fabric, agent_port)
add_fk!(fabric, agent_port_src, agent_src, :AgentPort!ap_agent => :Agent!Agent_id)
add_fk!(fabric, agent_port_src, port_src, :AgentPort!ap_port => :Port!Port_id)

# 2 agents per port
let agent_id = 0
for p in 1:30
    for _ in 1:2
        agent_id += 1
        if agent_id <= 60
            add_part!(fabric, :AgentPort, ap_agent=FK{AgentT}(agent_id), ap_port=FK{PortT}(p))
        end
    end
end
end

# ═══════════════════════════════════════════════════════════════════════
# TRANSACTION TABLES (SQLite)
# ═══════════════════════════════════════════════════════════════════════

# ─── VesselVisit (SQLite) ── vessel calls at a port ───
@present SchVesselVisit(FreeSchema) begin
    (Name, Vessel, Port, Berth, Pilot, Agent)::AttrType
    VesselVisit::Ob
    vv_vessel::Attr(VesselVisit, Vessel)
    vv_port::Attr(VesselVisit, Port)
    vv_berth::Attr(VesselVisit, Berth)
    vv_pilot::Attr(VesselVisit, Pilot)
    vv_agent::Attr(VesselVisit, Agent)
    vv_arrival::Attr(VesselVisit, Name)
    vv_departure::Attr(VesselVisit, Name)
    vv_status::Attr(VesselVisit, Name)
end
@acset_type VesselVisitT(SchVesselVisit)
vv_acset = VesselVisitT{Symbol, FK{VesselT}, FK{PortT}, FK{BerthT}, FK{PilotT}, FK{AgentT}}()
vv_db = DBSource(SQLite.DB(), vv_acset)
execute![τ](vv_db, FunSQL.render(vv_db, vv_acset))
vv_src = add_source!(fabric, vv_db, :VesselVisit)
add_fk!(fabric, vv_src, vessel_src, :VesselVisit!vv_vessel => :Vessel!Vessel_id)
add_fk!(fabric, vv_src, port_src, :VesselVisit!vv_port => :Port!Port_id)
add_fk!(fabric, vv_src, berth_src, :VesselVisit!vv_berth => :Berth!Berth_id)
add_fk!(fabric, vv_src, pilot_src, :VesselVisit!vv_pilot => :Pilot!Pilot_id)
add_fk!(fabric, vv_src, agent_src, :VesselVisit!vv_agent => :Agent!Agent_id)

# Generate 200 vessel visits
vv_records = []
for i in 1:200
    p = rand(1:30)
    v = rand(1:200)
    berth_start = (p - 1) * 5 + 1
    b = berth_start + rand(0:4)
    # pick a pilot licensed at this port
    pilot_at_port = 1 # TODO
    # pilot_at_port = filter(j -> begin
                               # pp_pilots = findall(x -> x == p, [getfield.(subpart(fabric, j, :pp_port), Ref(:val)) for j in 1:nparts(fabric, :PilotPort)])
    # end, 1:120)
    # simpler: pilot index roughly = port * 4 + offset
    pilot_base = (p - 1) * 4 + 1
    pi = min(pilot_base + rand(0:3), 120)
    # agent at port
    agent_base = (p - 1) * 2 + 1
    ag = min(agent_base + rand(0:1), 60)
    month = rand(1:12)
    day = rand(1:28)
    arr = Symbol("2024-$(lpad(month,2,'0'))-$(lpad(day,2,'0'))")
    dep = Symbol("2024-$(lpad(month,2,'0'))-$(lpad(min(day+rand(1:5),28),2,'0'))")
    status = rand([:completed, :in_port, :scheduled])
    push!(vv_records, (_id=i, vv_vessel=FK{VesselT}(v), vv_port=FK{PortT}(p),
        vv_berth=FK{BerthT}(b), vv_pilot=FK{PilotT}(pi), vv_agent=FK{AgentT}(ag),
        vv_arrival=arr, vv_departure=dep, vv_status=status))
end
for vv_record in vv_records
    add_part!(fabric, :VesselVisit, vv_record)
end

# ─── CrewMember (SQLite) ───
@present SchCrewMember(FreeSchema) begin
    (Name, Vessel, CrewRank)::AttrType
    CrewMember::Ob
    cm_vessel::Attr(CrewMember, Vessel)
    cm_rank::Attr(CrewMember, CrewRank)
    cm_name::Attr(CrewMember, Name)
end
@acset_type CrewMemberT(SchCrewMember)
cm_acset = CrewMemberT{Symbol, FK{VesselT}, FK{CrewRankT}}()
cm_db = DBSource(SQLite.DB(), cm_acset)
execute![τ](cm_db, FunSQL.render(cm_db, cm_acset))
cm_src = add_source!(fabric, cm_db, :CrewMember)
add_fk!(fabric, cm_src, vessel_src, :CrewMember!cm_vessel => :Vessel!Vessel_id)
add_fk!(fabric, cm_src, crew_rank_src, :CrewMember!cm_rank => :CrewRank!CrewRank_id)

# ~5 crew per vessel = 1000 crew
cm_records = []
let cm_id = 0
for v in 1:200
    for _ in 1:5
        cm_id += 1
        rk = rand(1:10)
        push!(cm_records, (_id=cm_id, cm_vessel=FK{VesselT}(v), cm_rank=FK{CrewRankT}(rk), cm_name=sym("Crew", cm_id)))
    end
end
end
for cm_record in cm_records
    add_part!(fabric, :CrewMember, cm_record)
end

# ─── CargoManifest (SQLite) ── cargo loaded for a vessel visit ───
@present SchCargoManifest(FreeSchema) begin
    (Name, VesselVisit, CargoType)::AttrType
    CargoManifest::Ob
    cmf_visit::Attr(CargoManifest, VesselVisit)
    cmf_cargo_type::Attr(CargoManifest, CargoType)
    cmf_tonnage::Attr(CargoManifest, Name)
end
@acset_type CargoManifestT(SchCargoManifest)
cmf_acset = CargoManifestT{Symbol, FK{VesselVisitT}, FK{CargoTypeT}}()
cmf_db = DBSource(SQLite.DB(), cmf_acset)
execute![τ](cmf_db, FunSQL.render(cmf_db, cmf_acset))
cmf_src = add_source!(fabric, cmf_db, :CargoManifest)
add_fk!(fabric, cmf_src, vv_src, :CargoManifest!cmf_visit => :VesselVisit!VesselVisit_id)
add_fk!(fabric, cmf_src, cargo_type_src, :CargoManifest!cmf_cargo_type => :CargoType!CargoType_id)

cmf_records = []
let cmf_id = 0
for visit in 1:200
    for _ in 1:rand(1:3)
        cmf_id += 1
        ct = rand(1:15)
        tons = Symbol(string(rand(100:50000)))
        push!(cmf_records, (_id=cmf_id, cmf_visit=FK{VesselVisitT}(visit), cmf_cargo_type=FK{CargoTypeT}(ct), cmf_tonnage=tons))
    end
end
end
for cmf_record in cmf_records
    add_part!(fabric, :CargoManifest, cmf_record)
end

# ─── Inspection (SQLite) ───
@present SchInspection(FreeSchema) begin
    (Name, VesselVisit, InspectionType)::AttrType
    Inspection::Ob
    ins_visit::Attr(Inspection, VesselVisit)
    ins_type::Attr(Inspection, InspectionType)
    ins_result::Attr(Inspection, Name)
    ins_date::Attr(Inspection, Name)
end
@acset_type InspectionT(SchInspection)
ins_acset = InspectionT{Symbol, FK{VesselVisitT}, FK{InspectionTypeT}}()
ins_db = DBSource(SQLite.DB(), ins_acset)
execute![τ](ins_db, FunSQL.render(ins_db, ins_acset))
ins_src = add_source!(fabric, ins_db, :Inspection)
add_fk!(fabric, ins_src, vv_src, :Inspection!ins_visit => :VesselVisit!VesselVisit_id)
add_fk!(fabric, ins_src, inspection_type_src, :Inspection!ins_type => :InspectionType!InspectionType_id)

ins_records = []
let ins_id = 0
for visit in 1:200
    if rand() < 0.6  # 60% of visits get inspected
        ins_id += 1
        it = rand(1:8)
        result = rand([:pass, :fail, :conditional_pass, :deficiency_noted])
        month = rand(1:12)
        day = rand(1:28)
        push!(ins_records, (_id=ins_id, ins_visit=FK{VesselVisitT}(visit), ins_type=FK{InspectionTypeT}(it),
            ins_result=result, ins_date=Symbol("2024-$(lpad(month,2,'0'))-$(lpad(day,2,'0'))")))
    end
end
end
for x in ins_records
    add_part!(fabric, :Inspection, x)
end

# ─── FuelTransaction (SQLite) ───
@present SchFuelTransaction(FreeSchema) begin
    (Name, VesselVisit, FuelType)::AttrType
    FuelTransaction::Ob
    ft_visit::Attr(FuelTransaction, VesselVisit)
    ft_fuel::Attr(FuelTransaction, FuelType)
    ft_quantity::Attr(FuelTransaction, Name)
end
@acset_type FuelTransactionT(SchFuelTransaction)
ft_acset = FuelTransactionT{Symbol, FK{VesselVisitT}, FK{FuelTypeT}}()
ft_db = DBSource(SQLite.DB(), ft_acset)
execute![τ](ft_db, FunSQL.render(ft_db, ft_acset))
ft_src = add_source!(fabric, ft_db, :FuelTransaction)
add_fk!(fabric, ft_src, vv_src, :FuelTransaction!ft_visit => :VesselVisit!VesselVisit_id)
add_fk!(fabric, ft_src, fuel_type_src, :FuelTransaction!ft_fuel => :FuelType!FuelType_id)

ft_records = []
let ft_id = 0
for visit in 1:200
    if rand() < 0.7  # 70% of visits refuel
        ft_id += 1
        fuel = rand(1:6)
        qty = Symbol(string(rand(50:5000)))
        push!(ft_records, (_id=ft_id, ft_visit=FK{VesselVisitT}(visit), ft_fuel=FK{FuelTypeT}(fuel), ft_quantity=qty))
    end
end
end
for x in ft_records
    add_part!(fabric, :FuelTransaction, x)
end

# ═══════════════════════════════════════════════════════════════════════
# QUERIES
# ═══════════════════════════════════════════════════════════════════════

# Cyclic query: "Domestic vessels with local pilots"
# Find vessels whose flag state country matches the port's country,
# where the assigned pilot is also licensed at that port.
# Filter by country.
#
# Cycle on `cty` (country) junction:
#   VesselCountry(vc_vessel=v, vc_country=cty)
#   PortCountry(ptc_port=pt, ptc_country=cty)
#
# Cycle on `pt` (port) junction:
#   VesselVisit(vv_port=pt, ...)
#   PilotPort(pp_port=pt, ...)
#
q_domestic = @relation (
        vessel_name=vn,
        port_name=ptn,
        pilot_name=pln,
        vv_arrival=arr
    ) begin
    # filter
    CountryFilter(country_name=cn)
    Country(id=cty, country_name=cn)
    # vessel flag state = country
    VesselCountry(vc_vessel=v, vc_country=cty)
    # port in same country
    PortCountry(ptc_port=pt, ptc_country=cty)
    # vessel visit at that port with a pilot
    VesselVisit(vv_vessel=v, vv_port=pt, vv_pilot=pl, vv_arrival=arr)
    # pilot licensed at that port
    PilotPort(pp_pilot=pl, pp_port=pt)
    # decode names
    Vessel(id=v, vessel_name=vn)
    Port(id=pt, port_name=ptn)
    Pilot(id=pl, pilot_name=pln)
end

using DataFrames

# To run:
@time q = prepare(q_domestic, fabric, filters=Dict(:country_name => :UnitedStates))
df = DataFrame(q)

# Simpler query: "What cargo types were loaded at each port?"
# q_cargo_by_port = @relation (port_name=ptn, cargo_type_name=ctn) begin
#     VesselVisit(id=vv, vv_port=pt)
#     CargoManifest(cmf_visit=vv, cmf_cargo_type=ct)
#     Port(id=pt, port_name=ptn)
#     CargoType(id=ct, cargo_type_name=ctn)
# end

# Star query: "For a given inspection result, find the vessel, port,
#   cargo types, and pilot involved"
# q_inspection_detail = @relation (
#         vessel_name=vn,
#         port_name=ptn,
#         pilot_name=pln,
#         cargo_type_name=ctn,
#         insp_result=ir,
#         insp_type=itn
#     ) begin
#     Inspection(ins_visit=vv, ins_type=it, ins_result=ir)
#     InspectionType(id=it, insp_type_name=itn)
#     VesselVisit(id=vv, vv_vessel=v, vv_port=pt, vv_pilot=pl)
#     CargoManifest(cmf_visit=vv, cmf_cargo_type=ct)
#     Vessel(id=v, vessel_name=vn)
#     Port(id=pt, port_name=ptn)
#     Pilot(id=pl, pilot_name=pln)
#     CargoType(id=ct, cargo_type_name=ctn)
# end
