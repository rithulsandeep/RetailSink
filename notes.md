# Phase 1:  Simulate Engine

## Inventory Simulator

### Data Schema & Fault Mapping

| Column Name | Potential Fault / Variation | Probability |
| :--- | :--- | :--- |
| `LogID` | None (Unique identifier) | 0% |
| `WarehouseCode` | None (Standardized) | 0% |
| `SKU_ID` | Missing SKU (Orphan logs) | 2% |
| `Item_Name` | None | 0% |
| `BatchNo` | None | 0% |
| `MovementType` | None | 0% |
| `Quantity_Change` | Logic Error (Negative value for INWARD movement) | 1% |
| `EventDate` | Inconsistent Date Formatting (e.g., "Feb 13, 2026") | 15% |
| `SupplierName` | None | 0% |
| `CountryOfOrigin` | None | 0% |
| `PackageWeight_kg` | None | 0% |
| `StorageTemp_C` | None | 0% |
| `BinLocation` | None | 0% |

## POS Simulator

### Data Schema & Fault Mapping

| Column Name | Potential Fault / Variation | Probability |
| :--- | :--- | :--- |
| `BillNo` | Multi-item bill (Duplicate BillNo) | 3% |
| `ItemCode` | None | 0% |
| `ProductName` | Leading/Trailing Whitespaces | 2% |
| `Qty` | Garbage strings ("NA", "?", "UNKNOWN") | 1% |
| `Qty` | Negative quantity | 1% |
| `BillDate` | Inconsistent date format ("%d-%m-%Y") | 10% |
| `Rate` | Garbage strings ("MISSING", "null") | 1% |
| `Rate` | Negative rate | 1% |
| `LoyaltyID` | Missing (Null) | 5% |
| `Nation` | None | 0% |
| `StoreCity` | Mixed casing (upper/lower) | 3% |
| `StoreCity` | Leading whitespace | 2% |
| `BuyPrice` | Linked to Rate (0 if rate is garbage) | - |
| `StockAtHand` | None | 0% |
| `PayMode` | Missing (Null) | 2% |
| `CashierID` | Garbage ID ("ERR_99") | 1% |
| `ShopTaxRate` | None | 0% |

## Web Simulator (Online Retail)

### Data Schema & Fault Mapping

| Column Name | Potential Fault / Variation | Probability |
| :--- | :--- | :--- |
| `InvoiceNo` | Cancellation (Prefix 'C') | 10% |
| `StockCode` | None | 0% |
| `Description` | None | 0% |
| `Quantity` | Negative quantity | 3% |
| `InvoiceDate` | None | 0% |
| `UnitPrice` | Missing (Null) | 2% |
| `CustomerID` | None | 0% |
| `Country` | Dynamic selection (UK, FR, DE, USA, etc.) | - |
| `City` | None | 0% |
| `Cost_Price` | None | 0% |
| `Initial_Stock_Level` | None | 0% |
| `Delivery_Date` | None | 0% |

## Shipment Simulator

### Data Schema & Fault Mapping

| Column Name | Potential Fault / Variation | Probability |
| :--- | :--- | :--- |
| `invoice_id` | Join from Web Simulator | - |
| `order_timestamp` | Join from Web Simulator | - |
| `ship_timestamp` | None (Generated +4-48h from order) | 0% |
| `delivery_timestamp` | None (Generated +2-7d from ship) | 0% |
| `city` | None | 0% |
| `country` | Dynamic selection (India, UK, USA, etc.) | - |

# Phase 2: Pipeline and Architecture

## Medallion Architecture

Medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally improving the structure and quality of data as it flows through each layer:

1.  **Bronze Layer (Raw)**:
    *   Acts as the staging area where data is stored in its raw format.
    *   Ensures that no data is lost and provides a historical record for "replayability" if ingestion logic needs to change.
2.  **Silver Layer (Validated/Enriched)**:
    *   Data is cleaned, filtered, and augmented.
    *   Synonyms are mapped, data types are enforced, and basic joins are performed to create a unified "Enterprise View".
3.  **Gold Layer (Aggregated/Business)**:
    *   Production-ready data optimized for high-performance consumption.
    *   Tables are structured (often using a Star Schema) to answer specific business KPIs like Daily Revenue or Inventory Turnover.

#### Why Medallion Architecture?

Compared to traditional "Source-to-BI" ETL or simple "Flat-File" storage, Medallion offers:

*   **Incremental Data Quality Improvement**:
    *   *How?* By design, the architecture mandates a tiered approach to processing. Data is never directly "warped" from raw to final aggregation; it must pass through validation and cleaning (Silver) first. This ensures that low-quality records are caught and handled logically before they ever reach business-facing tables.
*   **Data Replayability & Auditing**:
    *   *How?* By preserving a **Raw Layer (Bronze)**. If a business logic error is discovered in a Gold table six months later, the original source data is still available in its pristine state. We can "replay" the entire pipeline with corrected logic to reconstruct history accurately.
*   **Simplified Complexity Management**:
    *   *How?* Large, complex transformations are broken down into **Layer-Specific Tasks**. In Bronze, we only care about movement. In Silver, we only care about data integrity and standardization. In Gold, we only care about business definitions. This modularity makes the entire pipeline easier to maintain and debug.
*   **User Insulation**:
    *   *How?* Through **Abstraction**. End users and analysts are only given access to the Gold layer. This protects them from "schema drift" or temporary ingestion issues occurring at the source. The architecture acts as a buffer that ensures consumers only see a stable, curated version of the reality.
*   **Logical Traceability**:
    *   *How?* By creating **Permanent Checkpoints**. Because every state change is materialized in a new layer, you can literally "see" the transformation happen step-by-step. If a number looks wrong in Gold, you can check Silver to see if it was a cleaning error, or Bronze to see if it was a source data issue.

## Bronze Layer Implementation (`pipeline/bronze_layer.py`)

The Bronze Layer script is responsible for the **Ingestion (Raw)** phase, moving data from the `landing/` zone into the `medallion/bronze/` layer incrementally.

### 1. State Management (Checkpoints)
The script uses a checkpointing mechanism to ensure it only processes **new** data from files that are constantly growing.
*   **Checkpoint Storage**: Reads/writes to `data/pipeline_checkpoints.json`.
*   **Logic**: It stores the number of rows already processed for each source file. Next time it runs, it starts reading from where it left off, preventing duplicate data in the Lakehouse.

### 2. The Core Processing Logic (`process_file`)
This function handles the heavy lifting for each individual data source.

*   **Incremental CSV Reading**: Uses `pd.read_csv(..., skiprows=N)` where `N` is the last row count. It manually applies `custom_headers` since `skiprows` removes the file's original header.
*   **Data Validation (Minimal)**: 
    *   Converts the target timestamp column (e.g., `InvoiceDate`) to `datetime`.
    *   Drops rows with invalid/null timestamps to ensure partitioning works correctly.
*   **Derived Partitioning**: Creates `year`, `month`, and `day` columns from the timestamp for optimized storage and retrieval.
*   **Delta Lake Ingestion**:
    *   **Modes**: Uses `overwrite` for first-time runs and `append` for all subsequent runs.
    *   **Physical Layout**: Writes data in Delta Parquet format, partitioned by `/year/month/day/`.
    *   **Schema Evolution**: Uses `schema_mode="merge"` to allow the pipeline to adapt to new source columns automatically.

### 3. Schema Evolution & Integrity Strategy

The pipeline implements a **Code-First Schema Evolution** strategy. This is a deliberate design choice to balance system flexibility with data governance.

#### A. The "Code-First" Gatekeeper
While Delta Lake supports fully automatic schema discovery, we have **explicitly hardcoded headers** in the `__main__` block. 
*   **Security & Integrity**: This acts as a filter. If a source system accidentally includes sensitive data (e.g., PII) or garbage columns, the pipeline will ignore them because they aren't in the "allow-list" headers.
*   **The "Approval" Process**: To evolve the schema, a developer must manually update the header list in `bronze_layer.py`. This manual update serves as the official "approval" for the pipeline to start recognizing and ingesting the new field.

#### B. Storage-Level Evolution (The "Merge" Magic)
Once a new column is added to the hardcoded list, the storage layer handles the transition without any table downtime or manual SQL migrations:
*   **Automatic Merging**: When `write_deltalake` is called with `schema_mode="merge"`, Delta Lake detects the new column and updates the table's transaction log (metadata) to include it.
*   **Historical Consistency**: Existing data rows remain untouched, but are retroactively treated as having `NULL` values for the newly added column. This allows you to query the entire history (old and new) using a single schema immediately.

### 3. Data Ingestion Mapping

| Source System | Landing Path | Timestamp Column | Layer Path |
| :--- | :--- | :--- | :--- |
| **E-commerce (Web)** | `landing/Online_retail_data.csv` | `InvoiceDate` | `medallion/bronze/Online_retail_data` |
| **POS Billing (Store)** | `landing/pos_billing_data.csv` | `BillDate` | `medallion/bronze/pos_billing_data` |
| **Warehouse (Inventory)**| `landing/warehouse_inventory_data.csv` | `EventDate` | `medallion/bronze/warehouse_inventory_data` |
| **Shipments (Logistics)**| `landing/shipments_data.csv` | `ship_timestamp` | `medallion/bronze/shipments_data` |