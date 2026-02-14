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

### Why Medallion Architecture?

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

