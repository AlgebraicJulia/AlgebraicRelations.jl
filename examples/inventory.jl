using ACSets
using Catlab
using AlgebraicRelations

using SQLite, DBInterface
using FunSQL

using DataFrames

τ = AlgebraicRelations.SQL.DatabaseDS.DBSourceTrait()
fabric = DataFabric()

# ═══════════════════════════════════════════════════════════════════════
# Schema: Enterprise Inventory Management
#
# Entities:
#   Supplier, Warehouse, Product, Category, Customer, Region,
#   ShipMethod, QualityGrade, RecallReason
#
# Junction/Transaction tables:
#   PurchaseOrder (Supplier → Product)
#   Shipment (PurchaseOrder → Warehouse, via ShipMethod)
#   InventorySlot (Warehouse × Product — current stock)
#   SalesOrder (Customer → Product)
#   Delivery (SalesOrder → Warehouse, via ShipMethod)
#   ProductCategory (Product → Category)
#   Inspection (Shipment → QualityGrade)
#   Recall (Product → RecallReason)
#   SupplierRegion (Supplier → Region)
#   WarehouseRegion (Warehouse → Region)
#   CustomerRegion (Customer → Region)
#
# Cyclic query: "Find products where the supplier's region matches
#   the warehouse region AND the warehouse region matches the customer
#   region — i.e., locally sourced, stored, and sold products —
#   filtered to a specific region, returning product name, supplier,
#   warehouse, and customer."
#
#   Cycle: region ← SupplierRegion → Supplier → PurchaseOrder → Product
#          region ← WarehouseRegion → Warehouse ← Shipment ← PurchaseOrder
#          region ← CustomerRegion → Customer → SalesOrder → Product
#          (all three paths converge on the same region junction)
# ═══════════════════════════════════════════════════════════════════════

# ─── Region (InMemory) ───
@present SchRegion(FreeSchema) begin
    Name::AttrType
    Region::Ob
    region_name::Attr(Region, Name)
end
@acset_type RegionT(SchRegion)
region = InMemory(RegionT{Symbol}())
region_src = add_source!(fabric, region)

add_part!(fabric, :Region, region_name=:Northeast)   # 1
add_part!(fabric, :Region, region_name=:Southeast)   # 2
add_part!(fabric, :Region, region_name=:Midwest)     # 3
add_part!(fabric, :Region, region_name=:Southwest)   # 4
add_part!(fabric, :Region, region_name=:Pacific)     # 5

# ─── Category (InMemory) ───
@present SchCategory(FreeSchema) begin
    Name::AttrType
    Category::Ob
    category_name::Attr(Category, Name)
end
@acset_type CategoryT(SchCategory)
category = InMemory(CategoryT{Symbol}())
category_src = add_source!(fabric, category)

add_part!(fabric, :Category, category_name=:Electronics)     # 1
add_part!(fabric, :Category, category_name=:Furniture)       # 2
add_part!(fabric, :Category, category_name=:Food)            # 3
add_part!(fabric, :Category, category_name=:Clothing)        # 4
add_part!(fabric, :Category, category_name=:Automotive)      # 5
add_part!(fabric, :Category, category_name=:Pharmaceutical)  # 6
add_part!(fabric, :Category, category_name=:Industrial)      # 7

# ─── QualityGrade (InMemory) ───
@present SchQualityGrade(FreeSchema) begin
    Name::AttrType
    QualityGrade::Ob
    grade_name::Attr(QualityGrade, Name)
end
@acset_type QualityGradeT(SchQualityGrade)
quality_grade = InMemory(QualityGradeT{Symbol}())
quality_grade_src = add_source!(fabric, quality_grade)

add_part!(fabric, :QualityGrade, grade_name=:A_Excellent)  # 1
add_part!(fabric, :QualityGrade, grade_name=:B_Good)       # 2
add_part!(fabric, :QualityGrade, grade_name=:C_Fair)       # 3
add_part!(fabric, :QualityGrade, grade_name=:D_Poor)       # 4
add_part!(fabric, :QualityGrade, grade_name=:F_Rejected)   # 5

# ─── RecallReason (InMemory) ───
@present SchRecallReason(FreeSchema) begin
    Name::AttrType
    RecallReason::Ob
    reason::Attr(RecallReason, Name)
end
@acset_type RecallReasonT(SchRecallReason)
recall_reason = InMemory(RecallReasonT{Symbol}())
recall_reason_src = add_source!(fabric, recall_reason)

add_part!(fabric, :RecallReason, reason=:SafetyDefect)       # 1
add_part!(fabric, :RecallReason, reason=:Contamination)      # 2
add_part!(fabric, :RecallReason, reason=:LabelingError)      # 3
add_part!(fabric, :RecallReason, reason=:RegulatoryChange)   # 4

# ─── ShipMethod (InMemory) ───
@present SchShipMethod(FreeSchema) begin
    Name::AttrType
    ShipMethod::Ob
    method_name::Attr(ShipMethod, Name)
end
@acset_type ShipMethodT(SchShipMethod)
ship_method = InMemory(ShipMethodT{Symbol}())
ship_method_src = add_source!(fabric, ship_method)

add_part!(fabric, :ShipMethod, method_name=:Ground)     # 1
add_part!(fabric, :ShipMethod, method_name=:Air)        # 2
add_part!(fabric, :ShipMethod, method_name=:Rail)       # 3
add_part!(fabric, :ShipMethod, method_name=:Ocean)      # 4

# ─── Supplier (InMemory) ───
@present SchSupplier(FreeSchema) begin
    Name::AttrType
    Supplier::Ob
    supplier_name::Attr(Supplier, Name)
end
@acset_type SupplierT(SchSupplier)
supplier = InMemory(SupplierT{Symbol}())
supplier_src = add_source!(fabric, supplier)

add_part!(fabric, :Supplier, supplier_name=:AcmeParts)        # 1
add_part!(fabric, :Supplier, supplier_name=:GlobalFoods)      # 2
add_part!(fabric, :Supplier, supplier_name=:SteelWorks)       # 3
add_part!(fabric, :Supplier, supplier_name=:TextileCo)        # 4
add_part!(fabric, :Supplier, supplier_name=:ChipFab)          # 5
add_part!(fabric, :Supplier, supplier_name=:PharmaSource)     # 6
add_part!(fabric, :Supplier, supplier_name=:WoodCraft)        # 7
add_part!(fabric, :Supplier, supplier_name=:AutoComp)         # 8
add_part!(fabric, :Supplier, supplier_name=:FreshHarvest)     # 9
add_part!(fabric, :Supplier, supplier_name=:MicroElectron)    # 10

# ─── SupplierRegion (InMemory) ───
@present SchSupplierRegion(FreeSchema) begin
    (Supplier, Region)::AttrType
    SupplierRegion::Ob
    sr_supplier::Attr(SupplierRegion, Supplier)
    sr_region::Attr(SupplierRegion, Region)
end
@acset_type SupplierRegionT(SchSupplierRegion)
supplier_region = InMemory(SupplierRegionT{FK{SupplierT}, FK{RegionT}}())
supplier_region_src = add_source!(fabric, supplier_region)
add_fk!(fabric, supplier_region_src, supplier_src, :SupplierRegion!sr_supplier => :Supplier!Supplier_id)
add_fk!(fabric, supplier_region_src, region_src, :SupplierRegion!sr_region => :Region!Region_id)

add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(1),  sr_region=FK{RegionT}(3))  # AcmeParts: Midwest
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(2),  sr_region=FK{RegionT}(2))  # GlobalFoods: Southeast
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(2),  sr_region=FK{RegionT}(3))  # GlobalFoods: Midwest
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(3),  sr_region=FK{RegionT}(3))  # SteelWorks: Midwest
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(4),  sr_region=FK{RegionT}(2))  # TextileCo: Southeast
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(5),  sr_region=FK{RegionT}(5))  # ChipFab: Pacific
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(6),  sr_region=FK{RegionT}(1))  # PharmaSource: Northeast
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(7),  sr_region=FK{RegionT}(1))  # WoodCraft: Northeast
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(8),  sr_region=FK{RegionT}(3))  # AutoComp: Midwest
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(8),  sr_region=FK{RegionT}(4))  # AutoComp: Southwest
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(9),  sr_region=FK{RegionT}(5))  # FreshHarvest: Pacific
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(9),  sr_region=FK{RegionT}(4))  # FreshHarvest: Southwest
add_part!(fabric, :SupplierRegion, sr_supplier=FK{SupplierT}(10), sr_region=FK{RegionT}(5))  # MicroElectron: Pacific

# ─── Product (InMemory) ───
@present SchProduct(FreeSchema) begin
    Name::AttrType
    Product::Ob
    product_name::Attr(Product, Name)
    sku::Attr(Product, Name)
end
@acset_type ProductT(SchProduct)
product = InMemory(ProductT{Symbol}())
product_src = add_source!(fabric, product)

add_part!(fabric, :Product, product_name=:WidgetA,          sku=:SKU001)  # 1
add_part!(fabric, :Product, product_name=:GadgetB,          sku=:SKU002)  # 2
add_part!(fabric, :Product, product_name=:SteelBeam,        sku=:SKU003)  # 3
add_part!(fabric, :Product, product_name=:CottonShirt,      sku=:SKU004)  # 4
add_part!(fabric, :Product, product_name=:Microchip_X1,     sku=:SKU005)  # 5
add_part!(fabric, :Product, product_name=:Aspirin_100,      sku=:SKU006)  # 6
add_part!(fabric, :Product, product_name=:OakTable,         sku=:SKU007)  # 7
add_part!(fabric, :Product, product_name=:BrakeRotor,       sku=:SKU008)  # 8
add_part!(fabric, :Product, product_name=:OrganicApples,    sku=:SKU009)  # 9
add_part!(fabric, :Product, product_name=:CircuitBoard_V2,  sku=:SKU010)  # 10
add_part!(fabric, :Product, product_name=:WinterJacket,     sku=:SKU011)  # 11
add_part!(fabric, :Product, product_name=:CannedTomatoes,   sku=:SKU012)  # 12
add_part!(fabric, :Product, product_name=:Ibuprofen_200,    sku=:SKU013)  # 13
add_part!(fabric, :Product, product_name=:PineDresser,      sku=:SKU014)  # 14
add_part!(fabric, :Product, product_name=:SparkPlug,        sku=:SKU015)  # 15

# ─── ProductCategory (InMemory) ───
@present SchProductCategory(FreeSchema) begin
    (Product, Category)::AttrType
    ProductCategory::Ob
    pc_product::Attr(ProductCategory, Product)
    pc_category::Attr(ProductCategory, Category)
end
@acset_type ProductCategoryT(SchProductCategory)
product_category = InMemory(ProductCategoryT{FK{ProductT}, FK{CategoryT}}())
product_category_src = add_source!(fabric, product_category)
add_fk!(fabric, product_category_src, product_src, :ProductCategory!pc_product => :Product!Product_id)
add_fk!(fabric, product_category_src, category_src, :ProductCategory!pc_category => :Category!Category_id)

add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(1),  pc_category=FK{CategoryT}(1))  # WidgetA: Electronics
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(2),  pc_category=FK{CategoryT}(1))  # GadgetB: Electronics
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(3),  pc_category=FK{CategoryT}(7))  # SteelBeam: Industrial
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(4),  pc_category=FK{CategoryT}(4))  # CottonShirt: Clothing
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(5),  pc_category=FK{CategoryT}(1))  # Microchip_X1: Electronics
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(6),  pc_category=FK{CategoryT}(6))  # Aspirin_100: Pharmaceutical
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(7),  pc_category=FK{CategoryT}(2))  # OakTable: Furniture
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(8),  pc_category=FK{CategoryT}(5))  # BrakeRotor: Automotive
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(9),  pc_category=FK{CategoryT}(3))  # OrganicApples: Food
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(10), pc_category=FK{CategoryT}(1))  # CircuitBoard_V2: Electronics
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(11), pc_category=FK{CategoryT}(4))  # WinterJacket: Clothing
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(12), pc_category=FK{CategoryT}(3))  # CannedTomatoes: Food
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(13), pc_category=FK{CategoryT}(6))  # Ibuprofen_200: Pharmaceutical
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(14), pc_category=FK{CategoryT}(2))  # PineDresser: Furniture
add_part!(fabric, :ProductCategory, pc_product=FK{ProductT}(15), pc_category=FK{CategoryT}(5))  # SparkPlug: Automotive

# ─── Warehouse (InMemory) ───
@present SchWarehouse(FreeSchema) begin
    Name::AttrType
    Warehouse::Ob
    warehouse_name::Attr(Warehouse, Name)
end
@acset_type WarehouseT(SchWarehouse)
warehouse = InMemory(WarehouseT{Symbol}())
warehouse_src = add_source!(fabric, warehouse)

add_part!(fabric, :Warehouse, warehouse_name=:WH_Boston)       # 1
add_part!(fabric, :Warehouse, warehouse_name=:WH_Atlanta)      # 2
add_part!(fabric, :Warehouse, warehouse_name=:WH_Chicago)      # 3
add_part!(fabric, :Warehouse, warehouse_name=:WH_Dallas)       # 4
add_part!(fabric, :Warehouse, warehouse_name=:WH_Portland)     # 5
add_part!(fabric, :Warehouse, warehouse_name=:WH_Detroit)      # 6
add_part!(fabric, :Warehouse, warehouse_name=:WH_Phoenix)      # 7
add_part!(fabric, :Warehouse, warehouse_name=:WH_Seattle)      # 8

# ─── WarehouseRegion (InMemory) ───
@present SchWarehouseRegion(FreeSchema) begin
    (Warehouse, Region)::AttrType
    WarehouseRegion::Ob
    wr_warehouse::Attr(WarehouseRegion, Warehouse)
    wr_region::Attr(WarehouseRegion, Region)
end
@acset_type WarehouseRegionT(SchWarehouseRegion)
warehouse_region = InMemory(WarehouseRegionT{FK{WarehouseT}, FK{RegionT}}())
warehouse_region_src = add_source!(fabric, warehouse_region)
add_fk!(fabric, warehouse_region_src, warehouse_src, :WarehouseRegion!wr_warehouse => :Warehouse!Warehouse_id)
add_fk!(fabric, warehouse_region_src, region_src, :WarehouseRegion!wr_region => :Region!Region_id)

add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(1), wr_region=FK{RegionT}(1))  # Boston: Northeast
add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(2), wr_region=FK{RegionT}(2))  # Atlanta: Southeast
add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(3), wr_region=FK{RegionT}(3))  # Chicago: Midwest
add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(4), wr_region=FK{RegionT}(4))  # Dallas: Southwest
add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(5), wr_region=FK{RegionT}(5))  # Portland: Pacific
add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(6), wr_region=FK{RegionT}(3))  # Detroit: Midwest
add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(7), wr_region=FK{RegionT}(4))  # Phoenix: Southwest
add_part!(fabric, :WarehouseRegion, wr_warehouse=FK{WarehouseT}(8), wr_region=FK{RegionT}(5))  # Seattle: Pacific

# ─── Customer (InMemory) ───
@present SchCustomer(FreeSchema) begin
    Name::AttrType
    Customer::Ob
    customer_name::Attr(Customer, Name)
end
@acset_type CustomerT(SchCustomer)
customer = InMemory(CustomerT{Symbol}())
customer_src = add_source!(fabric, customer)

add_part!(fabric, :Customer, customer_name=:MegaMart)        # 1
add_part!(fabric, :Customer, customer_name=:TechDepot)       # 2
add_part!(fabric, :Customer, customer_name=:AutoZone)        # 3
add_part!(fabric, :Customer, customer_name=:FoodKing)        # 4
add_part!(fabric, :Customer, customer_name=:PharmaCorp)      # 5
add_part!(fabric, :Customer, customer_name=:BuildRight)      # 6
add_part!(fabric, :Customer, customer_name=:FashionHub)      # 7
add_part!(fabric, :Customer, customer_name=:WestCoastAuto)   # 8
add_part!(fabric, :Customer, customer_name=:HealthFirst)     # 9
add_part!(fabric, :Customer, customer_name=:HomeGoods)       # 10
add_part!(fabric, :Customer, customer_name=:FreshMart)       # 11
add_part!(fabric, :Customer, customer_name=:ChipBuyers)      # 12

# ─── CustomerRegion (InMemory) ───
@present SchCustomerRegion(FreeSchema) begin
    (Customer, Region)::AttrType
    CustomerRegion::Ob
    cr_customer::Attr(CustomerRegion, Customer)
    cr_region::Attr(CustomerRegion, Region)
end
@acset_type CustomerRegionT(SchCustomerRegion)
customer_region = InMemory(CustomerRegionT{FK{CustomerT}, FK{RegionT}}())
customer_region_src = add_source!(fabric, customer_region)
add_fk!(fabric, customer_region_src, customer_src, :CustomerRegion!cr_customer => :Customer!Customer_id)
add_fk!(fabric, customer_region_src, region_src, :CustomerRegion!cr_region => :Region!Region_id)

add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(1),  cr_region=FK{RegionT}(3))  # MegaMart: Midwest
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(1),  cr_region=FK{RegionT}(2))  # MegaMart: Southeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(2),  cr_region=FK{RegionT}(5))  # TechDepot: Pacific
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(2),  cr_region=FK{RegionT}(1))  # TechDepot: Northeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(3),  cr_region=FK{RegionT}(3))  # AutoZone: Midwest
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(3),  cr_region=FK{RegionT}(4))  # AutoZone: Southwest
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(4),  cr_region=FK{RegionT}(2))  # FoodKing: Southeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(4),  cr_region=FK{RegionT}(3))  # FoodKing: Midwest
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(5),  cr_region=FK{RegionT}(1))  # PharmaCorp: Northeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(6),  cr_region=FK{RegionT}(3))  # BuildRight: Midwest
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(7),  cr_region=FK{RegionT}(2))  # FashionHub: Southeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(8),  cr_region=FK{RegionT}(5))  # WestCoastAuto: Pacific
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(8),  cr_region=FK{RegionT}(4))  # WestCoastAuto: Southwest
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(9),  cr_region=FK{RegionT}(1))  # HealthFirst: Northeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(10), cr_region=FK{RegionT}(1))  # HomeGoods: Northeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(10), cr_region=FK{RegionT}(2))  # HomeGoods: Southeast
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(11), cr_region=FK{RegionT}(5))  # FreshMart: Pacific
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(11), cr_region=FK{RegionT}(4))  # FreshMart: Southwest
add_part!(fabric, :CustomerRegion, cr_customer=FK{CustomerT}(12), cr_region=FK{RegionT}(5))  # ChipBuyers: Pacific

# ─── PurchaseOrder (SQLite) ── supplier sells product ───
@present SchPurchaseOrder(FreeSchema) begin
    (Name, Supplier, Product)::AttrType
    PurchaseOrder::Ob
    po_supplier::Attr(PurchaseOrder, Supplier)
    po_product::Attr(PurchaseOrder, Product)
    po_date::Attr(PurchaseOrder, Name)
    po_qty::Attr(PurchaseOrder, Name)
end
@acset_type PurchaseOrderT(SchPurchaseOrder)
po_acset = PurchaseOrderT{Symbol, FK{SupplierT}, FK{ProductT}}()
po_db = DBSource(SQLite.DB(), po_acset)
execute![τ](po_db, FunSQL.render(po_db, po_acset))
po_src = add_source!(fabric, po_db, :PurchaseOrder)
add_fk!(fabric, po_src, supplier_src, :PurchaseOrder!po_supplier => :Supplier!Supplier_id)
add_fk!(fabric, po_src, product_src, :PurchaseOrder!po_product => :Product!Product_id)

add_part!(fabric, :PurchaseOrder, [
    (_id=1,  po_supplier=FK{SupplierT}(1),  po_product=FK{ProductT}(1),  po_date=Symbol("2024-01-15"), po_qty=Symbol("500")),
    (_id=2,  po_supplier=FK{SupplierT}(1),  po_product=FK{ProductT}(2),  po_date=Symbol("2024-01-20"), po_qty=Symbol("300")),
    (_id=3,  po_supplier=FK{SupplierT}(2),  po_product=FK{ProductT}(9),  po_date=Symbol("2024-02-01"), po_qty=Symbol("2000")),
    (_id=4,  po_supplier=FK{SupplierT}(2),  po_product=FK{ProductT}(12), po_date=Symbol("2024-02-05"), po_qty=Symbol("5000")),
    (_id=5,  po_supplier=FK{SupplierT}(3),  po_product=FK{ProductT}(3),  po_date=Symbol("2024-01-10"), po_qty=Symbol("100")),
    (_id=6,  po_supplier=FK{SupplierT}(4),  po_product=FK{ProductT}(4),  po_date=Symbol("2024-03-01"), po_qty=Symbol("1000")),
    (_id=7,  po_supplier=FK{SupplierT}(4),  po_product=FK{ProductT}(11), po_date=Symbol("2024-03-15"), po_qty=Symbol("800")),
    (_id=8,  po_supplier=FK{SupplierT}(5),  po_product=FK{ProductT}(5),  po_date=Symbol("2024-02-10"), po_qty=Symbol("10000")),
    (_id=9,  po_supplier=FK{SupplierT}(5),  po_product=FK{ProductT}(10), po_date=Symbol("2024-02-20"), po_qty=Symbol("5000")),
    (_id=10, po_supplier=FK{SupplierT}(6),  po_product=FK{ProductT}(6),  po_date=Symbol("2024-01-25"), po_qty=Symbol("20000")),
    (_id=11, po_supplier=FK{SupplierT}(6),  po_product=FK{ProductT}(13), po_date=Symbol("2024-02-15"), po_qty=Symbol("15000")),
    (_id=12, po_supplier=FK{SupplierT}(7),  po_product=FK{ProductT}(7),  po_date=Symbol("2024-03-10"), po_qty=Symbol("50")),
    (_id=13, po_supplier=FK{SupplierT}(7),  po_product=FK{ProductT}(14), po_date=Symbol("2024-03-20"), po_qty=Symbol("75")),
    (_id=14, po_supplier=FK{SupplierT}(8),  po_product=FK{ProductT}(8),  po_date=Symbol("2024-01-30"), po_qty=Symbol("2000")),
    (_id=15, po_supplier=FK{SupplierT}(8),  po_product=FK{ProductT}(15), po_date=Symbol("2024-02-28"), po_qty=Symbol("3000")),
    (_id=16, po_supplier=FK{SupplierT}(9),  po_product=FK{ProductT}(9),  po_date=Symbol("2024-04-01"), po_qty=Symbol("3000")),
    (_id=17, po_supplier=FK{SupplierT}(9),  po_product=FK{ProductT}(12), po_date=Symbol("2024-04-10"), po_qty=Symbol("4000")),
    (_id=18, po_supplier=FK{SupplierT}(10), po_product=FK{ProductT}(5),  po_date=Symbol("2024-03-05"), po_qty=Symbol("8000")),
    (_id=19, po_supplier=FK{SupplierT}(10), po_product=FK{ProductT}(10), po_date=Symbol("2024-03-25"), po_qty=Symbol("6000")),
    (_id=20, po_supplier=FK{SupplierT}(3),  po_product=FK{ProductT}(8),  po_date=Symbol("2024-04-15"), po_qty=Symbol("1500")),
])

# ─── Shipment (SQLite) ── PO shipped to warehouse ───
@present SchShipment(FreeSchema) begin
    (Name, PurchaseOrder, Warehouse, ShipMethod)::AttrType
    Shipment::Ob
    sh_po::Attr(Shipment, PurchaseOrder)
    sh_warehouse::Attr(Shipment, Warehouse)
    sh_method::Attr(Shipment, ShipMethod)
    sh_date::Attr(Shipment, Name)
end
@acset_type ShipmentT(SchShipment)
sh_acset = ShipmentT{Symbol, FK{PurchaseOrderT}, FK{WarehouseT}, FK{ShipMethodT}}()
sh_db = DBSource(SQLite.DB(), sh_acset)
execute![τ](sh_db, FunSQL.render(sh_db, sh_acset))
sh_src = add_source!(fabric, sh_db, :Shipment)
add_fk!(fabric, sh_src, po_src, :Shipment!sh_po => :PurchaseOrder!PurchaseOrder_id)
add_fk!(fabric, sh_src, warehouse_src, :Shipment!sh_warehouse => :Warehouse!Warehouse_id)
add_fk!(fabric, sh_src, ship_method_src, :Shipment!sh_method => :ShipMethod!ShipMethod_id)

add_part!(fabric, :Shipment, [
    (_id=1,  sh_po=FK{PurchaseOrderT}(1),  sh_warehouse=FK{WarehouseT}(3), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-01-20")),  # WidgetA → Chicago
    (_id=2,  sh_po=FK{PurchaseOrderT}(2),  sh_warehouse=FK{WarehouseT}(3), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-01-25")),  # GadgetB → Chicago
    (_id=3,  sh_po=FK{PurchaseOrderT}(3),  sh_warehouse=FK{WarehouseT}(2), sh_method=FK{ShipMethodT}(3), sh_date=Symbol("2024-02-05")),  # Apples → Atlanta
    (_id=4,  sh_po=FK{PurchaseOrderT}(4),  sh_warehouse=FK{WarehouseT}(3), sh_method=FK{ShipMethodT}(3), sh_date=Symbol("2024-02-10")),  # Tomatoes → Chicago
    (_id=5,  sh_po=FK{PurchaseOrderT}(5),  sh_warehouse=FK{WarehouseT}(6), sh_method=FK{ShipMethodT}(3), sh_date=Symbol("2024-01-15")),  # SteelBeam → Detroit
    (_id=6,  sh_po=FK{PurchaseOrderT}(6),  sh_warehouse=FK{WarehouseT}(2), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-03-05")),  # CottonShirt → Atlanta
    (_id=7,  sh_po=FK{PurchaseOrderT}(7),  sh_warehouse=FK{WarehouseT}(2), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-03-20")),  # WinterJacket → Atlanta
    (_id=8,  sh_po=FK{PurchaseOrderT}(8),  sh_warehouse=FK{WarehouseT}(5), sh_method=FK{ShipMethodT}(2), sh_date=Symbol("2024-02-15")),  # Microchip → Portland
    (_id=9,  sh_po=FK{PurchaseOrderT}(9),  sh_warehouse=FK{WarehouseT}(8), sh_method=FK{ShipMethodT}(2), sh_date=Symbol("2024-02-25")),  # CircuitBoard → Seattle
    (_id=10, sh_po=FK{PurchaseOrderT}(10), sh_warehouse=FK{WarehouseT}(1), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-01-30")),  # Aspirin → Boston
    (_id=11, sh_po=FK{PurchaseOrderT}(11), sh_warehouse=FK{WarehouseT}(1), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-02-20")),  # Ibuprofen → Boston
    (_id=12, sh_po=FK{PurchaseOrderT}(12), sh_warehouse=FK{WarehouseT}(1), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-03-15")),  # OakTable → Boston
    (_id=13, sh_po=FK{PurchaseOrderT}(13), sh_warehouse=FK{WarehouseT}(1), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-03-25")),  # PineDresser → Boston
    (_id=14, sh_po=FK{PurchaseOrderT}(14), sh_warehouse=FK{WarehouseT}(6), sh_method=FK{ShipMethodT}(3), sh_date=Symbol("2024-02-05")),  # BrakeRotor → Detroit
    (_id=15, sh_po=FK{PurchaseOrderT}(15), sh_warehouse=FK{WarehouseT}(4), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-03-05")),  # SparkPlug → Dallas
    (_id=16, sh_po=FK{PurchaseOrderT}(16), sh_warehouse=FK{WarehouseT}(5), sh_method=FK{ShipMethodT}(1), sh_date=Symbol("2024-04-05")),  # Apples → Portland
    (_id=17, sh_po=FK{PurchaseOrderT}(17), sh_warehouse=FK{WarehouseT}(4), sh_method=FK{ShipMethodT}(3), sh_date=Symbol("2024-04-15")),  # Tomatoes → Dallas
    (_id=18, sh_po=FK{PurchaseOrderT}(18), sh_warehouse=FK{WarehouseT}(8), sh_method=FK{ShipMethodT}(2), sh_date=Symbol("2024-03-10")),  # Microchip → Seattle
    (_id=19, sh_po=FK{PurchaseOrderT}(19), sh_warehouse=FK{WarehouseT}(5), sh_method=FK{ShipMethodT}(2), sh_date=Symbol("2024-04-01")),  # CircuitBoard → Portland
    (_id=20, sh_po=FK{PurchaseOrderT}(20), sh_warehouse=FK{WarehouseT}(3), sh_method=FK{ShipMethodT}(3), sh_date=Symbol("2024-04-20")),  # BrakeRotor → Chicago
])

# ─── Inspection (SQLite) ── quality check on shipment ───
@present SchInspection(FreeSchema) begin
    (Name, Shipment, QualityGrade)::AttrType
    Inspection::Ob
    insp_shipment::Attr(Inspection, Shipment)
    insp_grade::Attr(Inspection, QualityGrade)
    insp_notes::Attr(Inspection, Name)
end
@acset_type InspectionT(SchInspection)
insp_acset = InspectionT{Symbol, FK{ShipmentT}, FK{QualityGradeT}}()
insp_db = DBSource(SQLite.DB(), insp_acset)
execute![τ](insp_db, FunSQL.render(insp_db, insp_acset))
insp_src = add_source!(fabric, insp_db, :Inspection)
add_fk!(fabric, insp_src, sh_src, :Inspection!insp_shipment => :Shipment!Shipment_id)
add_fk!(fabric, insp_src, quality_grade_src, :Inspection!insp_grade => :QualityGrade!QualityGrade_id)

add_part!(fabric, :Inspection, [
    (_id=1,  insp_shipment=FK{ShipmentT}(1),  insp_grade=FK{QualityGradeT}(1), insp_notes=:clean),
    (_id=2,  insp_shipment=FK{ShipmentT}(2),  insp_grade=FK{QualityGradeT}(2), insp_notes=:minor_scratch),
    (_id=3,  insp_shipment=FK{ShipmentT}(3),  insp_grade=FK{QualityGradeT}(1), insp_notes=:fresh),
    (_id=4,  insp_shipment=FK{ShipmentT}(5),  insp_grade=FK{QualityGradeT}(1), insp_notes=:solid),
    (_id=5,  insp_shipment=FK{ShipmentT}(6),  insp_grade=FK{QualityGradeT}(3), insp_notes=:minor_stain),
    (_id=6,  insp_shipment=FK{ShipmentT}(8),  insp_grade=FK{QualityGradeT}(1), insp_notes=:pristine),
    (_id=7,  insp_shipment=FK{ShipmentT}(10), insp_grade=FK{QualityGradeT}(1), insp_notes=:sealed),
    (_id=8,  insp_shipment=FK{ShipmentT}(14), insp_grade=FK{QualityGradeT}(2), insp_notes=:surface_rust),
    (_id=9,  insp_shipment=FK{ShipmentT}(16), insp_grade=FK{QualityGradeT}(1), insp_notes=:organic_cert),
    (_id=10, insp_shipment=FK{ShipmentT}(18), insp_grade=FK{QualityGradeT}(4), insp_notes=:bent_pins),
    (_id=11, insp_shipment=FK{ShipmentT}(20), insp_grade=FK{QualityGradeT}(2), insp_notes=:slight_wear),
])

# ─── SalesOrder (SQLite) ── customer buys product ───
@present SchSalesOrder(FreeSchema) begin
    (Name, Customer, Product)::AttrType
    SalesOrder::Ob
    so_customer::Attr(SalesOrder, Customer)
    so_product::Attr(SalesOrder, Product)
    so_date::Attr(SalesOrder, Name)
    so_qty::Attr(SalesOrder, Name)
end
@acset_type SalesOrderT(SchSalesOrder)
so_acset = SalesOrderT{Symbol, FK{CustomerT}, FK{ProductT}}()
so_db = DBSource(SQLite.DB(), so_acset)
execute![τ](so_db, FunSQL.render(so_db, so_acset))
so_src = add_source!(fabric, so_db, :SalesOrder)
add_fk!(fabric, so_src, customer_src, :SalesOrder!so_customer => :Customer!Customer_id)
add_fk!(fabric, so_src, product_src, :SalesOrder!so_product => :Product!Product_id)

add_part!(fabric, :SalesOrder, [
    (_id=1,  so_customer=FK{CustomerT}(1),  so_product=FK{ProductT}(1),  so_date=Symbol("2024-02-01"), so_qty=Symbol("100")),   # MegaMart buys WidgetA
    (_id=2,  so_customer=FK{CustomerT}(1),  so_product=FK{ProductT}(12), so_date=Symbol("2024-02-10"), so_qty=Symbol("2000")),  # MegaMart buys Tomatoes
    (_id=3,  so_customer=FK{CustomerT}(2),  so_product=FK{ProductT}(5),  so_date=Symbol("2024-02-20"), so_qty=Symbol("5000")),  # TechDepot buys Microchip
    (_id=4,  so_customer=FK{CustomerT}(2),  so_product=FK{ProductT}(10), so_date=Symbol("2024-03-01"), so_qty=Symbol("3000")),  # TechDepot buys CircuitBoard
    (_id=5,  so_customer=FK{CustomerT}(3),  so_product=FK{ProductT}(8),  so_date=Symbol("2024-02-15"), so_qty=Symbol("500")),   # AutoZone buys BrakeRotor
    (_id=6,  so_customer=FK{CustomerT}(3),  so_product=FK{ProductT}(15), so_date=Symbol("2024-03-10"), so_qty=Symbol("1000")),  # AutoZone buys SparkPlug
    (_id=7,  so_customer=FK{CustomerT}(4),  so_product=FK{ProductT}(9),  so_date=Symbol("2024-02-25"), so_qty=Symbol("1500")),  # FoodKing buys Apples
    (_id=8,  so_customer=FK{CustomerT}(4),  so_product=FK{ProductT}(12), so_date=Symbol("2024-03-05"), so_qty=Symbol("3000")),  # FoodKing buys Tomatoes
    (_id=9,  so_customer=FK{CustomerT}(5),  so_product=FK{ProductT}(6),  so_date=Symbol("2024-02-05"), so_qty=Symbol("10000")), # PharmaCorp buys Aspirin
    (_id=10, so_customer=FK{CustomerT}(5),  so_product=FK{ProductT}(13), so_date=Symbol("2024-03-15"), so_qty=Symbol("8000")),  # PharmaCorp buys Ibuprofen
    (_id=11, so_customer=FK{CustomerT}(6),  so_product=FK{ProductT}(3),  so_date=Symbol("2024-02-10"), so_qty=Symbol("50")),    # BuildRight buys SteelBeam
    (_id=12, so_customer=FK{CustomerT}(7),  so_product=FK{ProductT}(4),  so_date=Symbol("2024-03-20"), so_qty=Symbol("500")),   # FashionHub buys CottonShirt
    (_id=13, so_customer=FK{CustomerT}(7),  so_product=FK{ProductT}(11), so_date=Symbol("2024-04-01"), so_qty=Symbol("400")),   # FashionHub buys WinterJacket
    (_id=14, so_customer=FK{CustomerT}(8),  so_product=FK{ProductT}(8),  so_date=Symbol("2024-03-25"), so_qty=Symbol("800")),   # WestCoastAuto buys BrakeRotor
    (_id=15, so_customer=FK{CustomerT}(8),  so_product=FK{ProductT}(15), so_date=Symbol("2024-04-05"), so_qty=Symbol("600")),   # WestCoastAuto buys SparkPlug
    (_id=16, so_customer=FK{CustomerT}(9),  so_product=FK{ProductT}(6),  so_date=Symbol("2024-03-01"), so_qty=Symbol("5000")),  # HealthFirst buys Aspirin
    (_id=17, so_customer=FK{CustomerT}(10), so_product=FK{ProductT}(7),  so_date=Symbol("2024-04-10"), so_qty=Symbol("20")),    # HomeGoods buys OakTable
    (_id=18, so_customer=FK{CustomerT}(10), so_product=FK{ProductT}(14), so_date=Symbol("2024-04-15"), so_qty=Symbol("30")),    # HomeGoods buys PineDresser
    (_id=19, so_customer=FK{CustomerT}(11), so_product=FK{ProductT}(9),  so_date=Symbol("2024-04-20"), so_qty=Symbol("2000")),  # FreshMart buys Apples
    (_id=20, so_customer=FK{CustomerT}(12), so_product=FK{ProductT}(5),  so_date=Symbol("2024-04-25"), so_qty=Symbol("4000")),  # ChipBuyers buys Microchip
    (_id=21, so_customer=FK{CustomerT}(12), so_product=FK{ProductT}(10), so_date=Symbol("2024-05-01"), so_qty=Symbol("2000")),  # ChipBuyers buys CircuitBoard
])

# ─── Delivery (SQLite) ── sales order fulfilled from warehouse ───
@present SchDelivery(FreeSchema) begin
    (Name, SalesOrder, Warehouse, ShipMethod)::AttrType
    Delivery::Ob
    del_so::Attr(Delivery, SalesOrder)
    del_warehouse::Attr(Delivery, Warehouse)
    del_method::Attr(Delivery, ShipMethod)
    del_date::Attr(Delivery, Name)
end
@acset_type DeliveryT(SchDelivery)
del_acset = DeliveryT{Symbol, FK{SalesOrderT}, FK{WarehouseT}, FK{ShipMethodT}}()
del_db = DBSource(SQLite.DB(), del_acset)
execute![τ](del_db, FunSQL.render(del_db, del_acset))
del_src = add_source!(fabric, del_db, :Delivery)
add_fk!(fabric, del_src, so_src, :Delivery!del_so => :SalesOrder!SalesOrder_id)
add_fk!(fabric, del_src, warehouse_src, :Delivery!del_warehouse => :Warehouse!Warehouse_id)
add_fk!(fabric, del_src, ship_method_src, :Delivery!del_method => :ShipMethod!ShipMethod_id)

add_part!(fabric, :Delivery, [
    (_id=1,  del_so=FK{SalesOrderT}(1),  del_warehouse=FK{WarehouseT}(3), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-02-05")),  # WidgetA from Chicago
    (_id=2,  del_so=FK{SalesOrderT}(2),  del_warehouse=FK{WarehouseT}(3), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-02-15")),  # Tomatoes from Chicago
    (_id=3,  del_so=FK{SalesOrderT}(3),  del_warehouse=FK{WarehouseT}(5), del_method=FK{ShipMethodT}(2), del_date=Symbol("2024-02-25")),  # Microchip from Portland
    (_id=4,  del_so=FK{SalesOrderT}(4),  del_warehouse=FK{WarehouseT}(8), del_method=FK{ShipMethodT}(2), del_date=Symbol("2024-03-05")),  # CircuitBoard from Seattle
    (_id=5,  del_so=FK{SalesOrderT}(5),  del_warehouse=FK{WarehouseT}(6), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-02-20")),  # BrakeRotor from Detroit
    (_id=6,  del_so=FK{SalesOrderT}(6),  del_warehouse=FK{WarehouseT}(4), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-03-15")),  # SparkPlug from Dallas
    (_id=7,  del_so=FK{SalesOrderT}(7),  del_warehouse=FK{WarehouseT}(2), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-03-01")),  # Apples from Atlanta
    (_id=8,  del_so=FK{SalesOrderT}(8),  del_warehouse=FK{WarehouseT}(3), del_method=FK{ShipMethodT}(3), del_date=Symbol("2024-03-10")),  # Tomatoes from Chicago
    (_id=9,  del_so=FK{SalesOrderT}(9),  del_warehouse=FK{WarehouseT}(1), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-02-10")),  # Aspirin from Boston
    (_id=10, del_so=FK{SalesOrderT}(10), del_warehouse=FK{WarehouseT}(1), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-03-20")),  # Ibuprofen from Boston
    (_id=11, del_so=FK{SalesOrderT}(11), del_warehouse=FK{WarehouseT}(6), del_method=FK{ShipMethodT}(3), del_date=Symbol("2024-02-15")),  # SteelBeam from Detroit
    (_id=12, del_so=FK{SalesOrderT}(12), del_warehouse=FK{WarehouseT}(2), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-03-25")),  # CottonShirt from Atlanta
    (_id=13, del_so=FK{SalesOrderT}(13), del_warehouse=FK{WarehouseT}(2), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-04-05")),  # WinterJacket from Atlanta
    (_id=14, del_so=FK{SalesOrderT}(14), del_warehouse=FK{WarehouseT}(3), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-04-01")),  # BrakeRotor from Chicago
    (_id=15, del_so=FK{SalesOrderT}(16), del_warehouse=FK{WarehouseT}(1), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-03-05")),  # Aspirin from Boston
    (_id=16, del_so=FK{SalesOrderT}(17), del_warehouse=FK{WarehouseT}(1), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-04-15")),  # OakTable from Boston
    (_id=17, del_so=FK{SalesOrderT}(18), del_warehouse=FK{WarehouseT}(1), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-04-20")),  # PineDresser from Boston
    (_id=18, del_so=FK{SalesOrderT}(19), del_warehouse=FK{WarehouseT}(5), del_method=FK{ShipMethodT}(1), del_date=Symbol("2024-04-25")),  # Apples from Portland
    (_id=19, del_so=FK{SalesOrderT}(20), del_warehouse=FK{WarehouseT}(8), del_method=FK{ShipMethodT}(2), del_date=Symbol("2024-05-01")),  # Microchip from Seattle
    (_id=20, del_so=FK{SalesOrderT}(21), del_warehouse=FK{WarehouseT}(5), del_method=FK{ShipMethodT}(2), del_date=Symbol("2024-05-05")),  # CircuitBoard from Portland
])

# ─── Recall (InMemory) ── product recalled for reason ───
@present SchRecall(FreeSchema) begin
    (Name, Product, RecallReason)::AttrType
    Recall::Ob
    rec_product::Attr(Recall, Product)
    rec_reason::Attr(Recall, RecallReason)
    rec_date::Attr(Recall, Name)
end
@acset_type RecallT(SchRecall)
recall = InMemory(RecallT{Symbol, FK{ProductT}, FK{RecallReasonT}}())
recall_src = add_source!(fabric, recall)
add_fk!(fabric, recall_src, product_src, :Recall!rec_product => :Product!Product_id)
add_fk!(fabric, recall_src, recall_reason_src, :Recall!rec_reason => :RecallReason!RecallReason_id)

add_part!(fabric, :Recall, rec_product=FK{ProductT}(6),  rec_reason=FK{RecallReasonT}(3), rec_date=Symbol("2024-04-01"))  # Aspirin: LabelingError
add_part!(fabric, :Recall, rec_product=FK{ProductT}(12), rec_reason=FK{RecallReasonT}(2), rec_date=Symbol("2024-05-10"))  # Tomatoes: Contamination
add_part!(fabric, :Recall, rec_product=FK{ProductT}(8),  rec_reason=FK{RecallReasonT}(1), rec_date=Symbol("2024-06-01"))  # BrakeRotor: SafetyDefect


# ═══════════════════════════════════════════════════════════════════════
# Queries
# ═══════════════════════════════════════════════════════════════════════

# Cyclic query: "Find locally sourced, stored, and sold products in the Midwest"
#
# The cycle: all three region paths (supplier, warehouse, customer)
# converge on the same region junction `rg`:
#
#   SupplierRegion(sr_supplier=s, sr_region=rg)
#   → Supplier → PurchaseOrder → Product ← SalesOrder ← Customer
#   WarehouseRegion(wr_warehouse=wh, wr_region=rg)
#   → Warehouse ← Shipment ← PurchaseOrder
#   → Warehouse ← Delivery → SalesOrder
#   CustomerRegion(cr_customer=c, cr_region=rg)
#
# Filter: region_name = :Midwest
#
# Expected Midwest results:
#   AcmeParts (Midwest supplier) → WidgetA/GadgetB → shipped to Chicago (Midwest)
#     → sold to MegaMart (Midwest customer) for WidgetA
#     → sold to MegaMart/FoodKing (Midwest) for Tomatoes from Chicago
#   SteelWorks (Midwest) → SteelBeam → Detroit (Midwest) → BuildRight (Midwest)
#   AutoComp (Midwest) → BrakeRotor → Detroit/Chicago (Midwest) → AutoZone (Midwest)
#   GlobalFoods (Midwest) → Tomatoes → Chicago (Midwest) → MegaMart/FoodKing (Midwest)
#
q_local_supply = @relation (
        product_name=pn,
        supplier_name=sn,
        warehouse_name=whn,
        customer_name=cn,
        # region_name=rgn # TODO currently outwire cannot be displayed
    ) begin
    # filter
    RegionFilter(region_name=rgn)
    Region(id=rg, region_name=rgn)
    # supplier path
    SupplierRegion(sr_supplier=s, sr_region=rg)
    Supplier(id=s, supplier_name=sn)
    # warehouse path (inbound)
    Shipment(sh_po=po, sh_warehouse=wh)
    PurchaseOrder(id=po, po_supplier=s, po_product=p)
    WarehouseRegion(wr_warehouse=wh, wr_region=rg)
    Warehouse(id=wh, warehouse_name=whn)
    # customer path
    Delivery(del_so=so, del_warehouse=wh)
    SalesOrder(id=so, so_customer=c, so_product=p)
    CustomerRegion(cr_customer=c, cr_region=rg)
    Customer(id=c, customer_name=cn)
    # product
    Product(id=p, product_name=pn)
end

q = q_local_supply(fabric, filters=Dict(:region_name => :Midwest))
q
