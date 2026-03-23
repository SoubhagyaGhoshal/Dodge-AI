"""
Data ingestion script: reads all JSONL files from sap-o2c-data/ and loads them
into a SQLite database (o2c.db) with clean, typed tables.
"""
import json
import sqlite3
import os
import glob

DATA_DIR = os.path.join(os.path.dirname(__file__), "sap-o2c-data")
DB_PATH = os.path.join(os.path.dirname(__file__), "o2c.db")

# Maps folder name → SQL table name + schema
TABLE_DEFINITIONS = {
    "sales_order_headers": """
        CREATE TABLE IF NOT EXISTS sales_order_headers (
            salesOrder TEXT PRIMARY KEY,
            salesOrderType TEXT,
            salesOrganization TEXT,
            distributionChannel TEXT,
            organizationDivision TEXT,
            salesGroup TEXT,
            salesOffice TEXT,
            soldToParty TEXT,
            creationDate TEXT,
            createdByUser TEXT,
            lastChangeDateTime TEXT,
            totalNetAmount REAL,
            overallDeliveryStatus TEXT,
            overallOrdReltdBillgStatus TEXT,
            overallSdDocReferenceStatus TEXT,
            transactionCurrency TEXT,
            pricingDate TEXT,
            requestedDeliveryDate TEXT,
            headerBillingBlockReason TEXT,
            deliveryBlockReason TEXT,
            incotermsClassification TEXT,
            incotermsLocation1 TEXT,
            customerPaymentTerms TEXT,
            totalCreditCheckStatus TEXT
        )
    """,
    "sales_order_items": """
        CREATE TABLE IF NOT EXISTS sales_order_items (
            salesOrder TEXT,
            salesOrderItem TEXT,
            salesOrderItemCategory TEXT,
            material TEXT,
            requestedQuantity REAL,
            requestedQuantityUnit TEXT,
            transactionCurrency TEXT,
            netAmount REAL,
            materialGroup TEXT,
            productionPlant TEXT,
            storageLocation TEXT,
            salesDocumentRjcnReason TEXT,
            itemBillingBlockReason TEXT,
            PRIMARY KEY (salesOrder, salesOrderItem)
        )
    """,
    "sales_order_schedule_lines": """
        CREATE TABLE IF NOT EXISTS sales_order_schedule_lines (
            salesOrder TEXT,
            salesOrderItem TEXT,
            scheduleLine TEXT,
            requestedDeliveryDate TEXT,
            confirmedDeliveryDate TEXT,
            orderQuantity REAL,
            confirmedOrderQuantity REAL,
            deliveredQtyInOrderQtyUnit REAL,
            openDeliveryQuantity REAL,
            quantityUnit TEXT,
            PRIMARY KEY (salesOrder, salesOrderItem, scheduleLine)
        )
    """,
    "billing_document_headers": """
        CREATE TABLE IF NOT EXISTS billing_document_headers (
            billingDocument TEXT PRIMARY KEY,
            billingDocumentType TEXT,
            creationDate TEXT,
            lastChangeDateTime TEXT,
            billingDocumentDate TEXT,
            billingDocumentIsCancelled INTEGER,
            cancelledBillingDocument TEXT,
            totalNetAmount REAL,
            transactionCurrency TEXT,
            companyCode TEXT,
            fiscalYear TEXT,
            accountingDocument TEXT,
            soldToParty TEXT
        )
    """,
    "billing_document_items": """
        CREATE TABLE IF NOT EXISTS billing_document_items (
            billingDocument TEXT,
            billingDocumentItem TEXT,
            salesDocument TEXT,
            salesDocumentItem TEXT,
            referenceDocument TEXT,
            referenceDocumentItem TEXT,
            material TEXT,
            billingQuantity REAL,
            billingQuantityUnit TEXT,
            netAmount REAL,
            transactionCurrency TEXT,
            plant TEXT,
            PRIMARY KEY (billingDocument, billingDocumentItem)
        )
    """,
    "billing_document_cancellations": """
        CREATE TABLE IF NOT EXISTS billing_document_cancellations (
            billingDocument TEXT PRIMARY KEY,
            cancellationDocument TEXT,
            cancelledBillingDocument TEXT,
            cancellationReason TEXT
        )
    """,
    "outbound_delivery_headers": """
        CREATE TABLE IF NOT EXISTS outbound_delivery_headers (
            outboundDelivery TEXT PRIMARY KEY,
            shippingPoint TEXT,
            deliveryDate TEXT,
            actualGoodsMovementDate TEXT,
            overallDeliveryStatus TEXT,
            totalGrossWeight REAL,
            totalNetWeight REAL,
            weightUnit TEXT
        )
    """,
    "outbound_delivery_items": """
        CREATE TABLE IF NOT EXISTS outbound_delivery_items (
            outboundDelivery TEXT,
            outboundDeliveryItem TEXT,
            referenceDocument TEXT,
            referenceDocumentItem TEXT,
            salesOrder TEXT,
            salesOrderItem TEXT,
            material TEXT,
            actualDeliveredQuantityInBaseUnit REAL,
            deliveryQuantityUnit TEXT,
            plant TEXT,
            storageLocation TEXT,
            batch TEXT,
            PRIMARY KEY (outboundDelivery, outboundDeliveryItem)
        )
    """,
    "business_partners": """
        CREATE TABLE IF NOT EXISTS business_partners (
            businessPartner TEXT PRIMARY KEY,
            businessPartnerName TEXT,
            businessPartnerType TEXT,
            businessPartnerGrouping TEXT,
            businessPartnerCategory TEXT,
            creationDate TEXT,
            country TEXT,
            region TEXT,
            city TEXT,
            postalCode TEXT,
            streetName TEXT
        )
    """,
    "business_partner_addresses": """
        CREATE TABLE IF NOT EXISTS business_partner_addresses (
            businessPartner TEXT,
            addressId TEXT,
            streetName TEXT,
            cityName TEXT,
            postalCode TEXT,
            country TEXT,
            region TEXT,
            fullAddress TEXT,
            PRIMARY KEY (businessPartner, addressId)
        )
    """,
    "customer_company_assignments": """
        CREATE TABLE IF NOT EXISTS customer_company_assignments (
            customer TEXT,
            companyCode TEXT,
            paymentTerms TEXT,
            accountGroup TEXT,
            reconciliationAccount TEXT,
            PRIMARY KEY (customer, companyCode)
        )
    """,
    "customer_sales_area_assignments": """
        CREATE TABLE IF NOT EXISTS customer_sales_area_assignments (
            customer TEXT,
            salesOrganization TEXT,
            distributionChannel TEXT,
            division TEXT,
            customerGroup TEXT,
            deliveryPriority TEXT,
            shippingCondition TEXT,
            PRIMARY KEY (customer, salesOrganization, distributionChannel, division)
        )
    """,
    "journal_entry_items_accounts_receivable": """
        CREATE TABLE IF NOT EXISTS journal_entry_items_accounts_receivable (
            companyCode TEXT,
            fiscalYear TEXT,
            accountingDocument TEXT,
            ledgerGLLineItem TEXT,
            glAccount TEXT,
            businessPartner TEXT,
            amountInCompanyCodeCurrency REAL,
            companyCodeCurrency TEXT,
            documentDate TEXT,
            postingDate TEXT,
            referenceDocument TEXT,
            PRIMARY KEY (companyCode, fiscalYear, accountingDocument, ledgerGLLineItem)
        )
    """,
    "payments_accounts_receivable": """
        CREATE TABLE IF NOT EXISTS payments_accounts_receivable (
            companyCode TEXT,
            fiscalYear TEXT,
            accountingDocument TEXT,
            ledgerGLLineItem TEXT,
            businessPartner TEXT,
            amountInCompanyCodeCurrency REAL,
            companyCodeCurrency TEXT,
            paymentDocument TEXT,
            paymentReference TEXT,
            clearingDate TEXT,
            documentDate TEXT,
            PRIMARY KEY (companyCode, fiscalYear, accountingDocument, ledgerGLLineItem)
        )
    """,
    "plants": """
        CREATE TABLE IF NOT EXISTS plants (
            plant TEXT PRIMARY KEY,
            plantName TEXT,
            companyCode TEXT,
            country TEXT,
            region TEXT,
            city TEXT,
            streetName TEXT,
            postalCode TEXT,
            language TEXT,
            systemInternalPlanningType TEXT
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            product TEXT PRIMARY KEY,
            productType TEXT,
            productGroup TEXT,
            baseUnit TEXT,
            grossWeight REAL,
            netWeight REAL,
            weightUnit TEXT,
            creationDate TEXT,
            lastChangeDate TEXT
        )
    """,
    "product_descriptions": """
        CREATE TABLE IF NOT EXISTS product_descriptions (
            product TEXT,
            language TEXT,
            productDescription TEXT,
            PRIMARY KEY (product, language)
        )
    """,
    "product_plants": """
        CREATE TABLE IF NOT EXISTS product_plants (
            product TEXT,
            plant TEXT,
            profileCode TEXT,
            profileValidityStartDate TEXT,
            availabilityCheckType TEXT,
            PRIMARY KEY (product, plant)
        )
    """,
    "product_storage_locations": """
        CREATE TABLE IF NOT EXISTS product_storage_locations (
            product TEXT,
            plant TEXT,
            storageLocation TEXT,
            warehouseStorageBin TEXT,
            PRIMARY KEY (product, plant, storageLocation)
        )
    """,
}

def flatten_json(obj, prefix=""):
    """Flatten nested JSON (e.g. creationTime: {hours, minutes, seconds} → creationTime_hours etc.)"""
    items = {}
    for k, v in obj.items():
        new_key = f"{prefix}{k}" if not prefix else f"{prefix}_{k}"
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key))
        else:
            items[new_key] = v
    return items

def load_jsonl_files(folder_path):
    """Load all JSONL files from a folder into a list of dicts."""
    records = []
    pattern = os.path.join(folder_path, "*.jsonl")
    for fpath in glob.glob(pattern):
        with open(fpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        raw = json.loads(line)
                        records.append(flatten_json(raw))
                    except json.JSONDecodeError:
                        pass
    return records

def safe_insert(conn, table_name, records, schema_sql):
    """Insert records into a table, skipping unknown columns and handling conflicts."""
    if not records:
        print(f"  No records for {table_name}")
        return
    
    # Get actual columns from the CREATE TABLE statement
    conn.execute(schema_sql)
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    db_columns = {row[1] for row in cursor.fetchall()}
    
    inserted = 0
    for record in records:
        # Filter record to only known columns, convert booleans
        filtered = {}
        for col in db_columns:
            if col in record:
                val = record[col]
                if isinstance(val, bool):
                    val = int(val)
                filtered[col] = val
        
        if not filtered:
            continue
        
        cols = ", ".join(filtered.keys())
        placeholders = ", ".join(["?" for _ in filtered])
        try:
            conn.execute(
                f"INSERT OR IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})",
                list(filtered.values())
            )
            inserted += 1
        except sqlite3.Error as e:
            pass  # skip bad rows silently
    
    print(f"  ✓ {table_name}: {inserted} records inserted")


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    
    for folder_name, schema_sql in TABLE_DEFINITIONS.items():
        folder_path = os.path.join(DATA_DIR, folder_name)
        print(f"\nProcessing: {folder_name}")
        
        if not os.path.exists(folder_path):
            print(f"  WARNING: folder not found: {folder_path}")
            conn.execute(schema_sql)
            continue
        
        records = load_jsonl_files(folder_path)
        print(f"  Loaded {len(records)} raw records")
        safe_insert(conn, folder_name, records, schema_sql)
    
    conn.commit()
    conn.close()
    print(f"\n✅ Database created: {DB_PATH}")

    # Print summary
    conn = sqlite3.connect(DB_PATH)
    print("\n📊 Table row counts:")
    for table in TABLE_DEFINITIONS.keys():
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")
        except:
            pass
    conn.close()


if __name__ == "__main__":
    main()
