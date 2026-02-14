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


## Silver Layer Implementation (`pipeline/silver_layer.py`)

The Silver Layer is the **Clean & Curated** zone. It transitions data from the raw, siloed **Bronze** tables into a unified **Enterprise View**, enforcing strict data quality and business logic.

### 1. The ELT Paradigm vs. ETL
Our architecture follows the **ELT (Extract, Load, Transform)** pattern, which is fundamentally superior for modern Lakehouses:
*   **ETL (Traditional)**: Transforms data *before* it hits storage. If the transformation logic has a bug, the data is lost forever.
*   **ELT (Our Way)**: We **Extract** from the source, **Load** it raw into Bronze, and then **Transform** it into Silver.
*   **Why it's better**: Because the raw data is safely in Bronze, we can **replay** the transformations as many times as we want. If we decide to change a business rule, we just re-run the Silver pipeline.

### 2. DuckDB: The In-Process SQL Powerhouse
We use **DuckDB** as our primary transformation engine instead of pure Python loops or Pandas.
*   **Why DuckDB?**: It is a vectorized columnar engine designed for analytics. It runs "in-process" (no server to manage) but performs like a massive data warehouse.
*   **Why SQL Queries?**: SQL is **declarative**. DuckDB's optimizer can look at a SQL query and figure out the most efficient way to execute it (e.g., reordering joins, push-down filters).
*   **Optimized Patterns**: Our queries use `QUALIFY ROW_NUMBER()` which is the most efficient way to perform deduplication in a single pass over the data.

### 3. Apache Arrow: The Zero-Copy Highway
To move data from DuckDB (where it's transformed) to Delta Lake (where it's stored), we use **Apache Arrow**.
*   **What it is**: A cross-language memory format for columnar data.
*   **Why we use it**: Normally, moving data between two libraries involves "serialization" (converting to a string/format and back). Arrow allows DuckDB to hand over the memory address of the data directly to the Delta writer. It's **"Zero-Copy"**, meaning the data never actually moves; the libraries just share the memory.

### 4. Silver Layer "Fix List" (Data Normalization)
The Silver layer is responsible for "fixing" the messy data generated by our simulators. Every query in `silver_layer.py` implements these fixes:

| Fix Type | Implementation Mechanic | Purpose |
| :--- | :--- | :--- |
| **Normalization** | `TRIM(UPPER(column))` | Removes whitespace and ensures case-consistency (e.g., 'UK' vs 'uk'). |
| **Casting** | `::INTEGER`, `::DOUBLE` | Enforces strict data types, converting strings to usable numbers. |
| **Null Handling** | `COALESCE(NULLIF(..., ''), 'Unknown')` | Replaces empty strings and nulls with meaningful defaults. |
| **Logic Cleanup** | `REGEXP_REPLACE(..., '^C', '')` | Strips e-commerce prefixes like 'C' from Invoice IDs for joining. |
| **Outlier Filter** | `WHERE UnitPrice > 0` | Discards garbage records with negative or zero prices. |
| **Deduplication** | `PARTITION BY (invoice_id, product_id, ...)` | Ensures only one "Source of Truth" exists for every transaction. |

### 5. Progressive Upsert Strategy (`merge_to_silver`)
Unlike Bronze (which mainly appends), Silver often performs an **Upsert (MERGE)**:
*   **Predicate Logic**: It matches incoming records with existing records using `unique_keys` (like `invoice_id`).
*   **Action**: 
    *   If a match is found: It **updates** the existing record (handling corrections).
    *   If no match is found: It **inserts** the new record.
*   **Partition Pruning**: The merge includes `year`, `month`, and `day` in the predicate, allowing Delta Lake to skip entire directories of data it doesn't need to check.

### 6. How Delta Lake Ensures ACID Properties
Delta Lake doesn't just promise ACID; it implements it through the **Delta Log (`_delta_log`)**, a transaction log that tracks every change to the table.

*   **Atomicity (via Log Commits)**: Every Silver update is treated as a single "commit" (a JSON file in the `_delta_log`). If the `merge` operation fails mid-way, the JSON commit file is never created, and the data files remain "invisible" to the system. It’s all-or-nothing.
*   **Consistency (via Schema Enforcement)**: Delta Lake acts as a guard. Before any data hits the Silver layer, it validates the incoming schema against the metadata stored in the log. If a column type or name doesn't match the "contract," the transaction is aborted before any corruption can occur.
*   **Isolation (via Snapshot Isolation)**: When our Dashboard or API reads from Silver, the log tells it exactly which Parquet files belong to the "Current Version." Even if the Silver pipeline is actively writing new files, the reader stays on its stable version. No one ever sees a "half-written" or inconsistent table.
*   **Durability (via Persistent Logs)**: Because the transaction log is stored directly on the filesystem (next to the Parquet files), the state is never in memory alone. Even if the server crashes, the log provides a perfect audit trail to reconstruct the exact state of the data.

### 7. Future Goal: Dynamic Schema Evolution
Currently, Silver relies on hardcoded SQL templates to ensure strict "approval" of data. To make this evolutionary without manual intervention:
1.  **Header Fetching**: We can modify the pipeline to use `DESCRIBE` on the Bronze table to automatically build the `SELECT` list.
2.  **Pass-through**: Any column found in Bronze that isn't in our "Fix List" would be passed through as-is, while known columns still get the normalization/cleaning treatment.

## Gold Layer Implementation (`pipeline/gold_layer.py`)

The Gold Layer is the **Business-Ready** zone. It transforms the unified Silver tables into a highly optimized **Star Schema** designed specifically for high-performance BI dashboards and executive reporting.

### 1. The Star Schema Design
The Gold layer abandons the wide-table format of Silver in favor of a Star Schema. While other patterns exist, the Star Schema was chosen for its specific balance of performance and usability in the Retail context.

#### Why Star Schema (vs. others)?
*   **Over Flat Tables (Wide Tables)**: Silver tables are "flat" (everything in one row). While easy to read, they are inefficient for large-scale analytics because repeating long strings (like product descriptions) for every sale bloats storage and slows down aggregations. **Star Schema** replaces these strings with small integers (Keys), making the data 10x more compact and faster to scan.
*   **Over Snowflake Schema**: Snowflake schemas normalize data even further (e.g., splitting `dim_product` into `dim_category` and `dim_supplier`). This complexity requires much more code to maintain and forces the Dashboard to perform "chained joins," which hurts interactive query performance. **Star Schema** is the "Goldilocks" zone—it's normalized enough for speed but simple enough for easy SQL reporting.
*   **Simplicity for Reporting**: BI tools (like our Dashboard) are natively designed to understand Star Schemas. It allows users to "slice and dice" data intuitively without needing to understand complex data relationships.

*   **Fact Tables**: Contain the measurable, quantitative data for business processes (e.g., `fact_sales`, `fact_inventory`, `fact_shipments`).
*   **Dimension Tables**: Contain the descriptive context (e.g., `dim_product`, `dim_customer`, `dim_date`).
*   **Surrogate Keys**: We use deterministic hashing (`hash()` or `MD5()`) to create unique integer-based keys (e.g., `product_key`). This makes the "Fact-to-Dim" joins significantly faster than using long string-based IDs.

### 2. Business Rationale: Selection of Facts & Dimensions
In a retail environment, we chose these specific tables to provide a 360-degree view of the business: Commercial (Sales), Operational (Inventory), and Logistics (Shipments).

#### A. Fact Tables (The "What Happened?")
*   **`fact_sales`**: Represents the **Commercial Heart** of the business. It captures every transaction from both Online and POS systems.
    *   *Purpose:* To calculate Revenue, Profit Margins (via `cost_price`), and Volume. It allows us to answer: *"Which city is our best performer this month?"*
*   **`fact_inventory`**: Represents **Operational Health**. It tracks movement types (Inward/Outward) and quantities.
    *   *Purpose:* To monitor stock levels, calculate "Inventory Turnover Ratio," and prevent stockouts. It answers: *"Are we overstocked on slow-moving products?"*
*   **`fact_shipments`**: Represents **Customer Satisfaction**. It connects sales to logistics.
    *   *Purpose:* To measure delivery performance and logistics efficiency. It answers: *"Is our 2-day delivery promise actually being met across all countries?"*

#### B. Dimension Tables (The "Who, Where, and When?")
*   **`dim_product`**: The **Product Catalog**. Unified from all sources (ERP, POS, WMS).
    *   *Purpose:* Provides the descriptive attributes (names, categories) needed to slice sales data. Without this, we only have IDs like `84029E`, not names like *"Heart Hanging Lantern."*
*   **`dim_customer`**: The **Customer Profile**. Tracks profile changes via SCD Type 2.
    *   *Purpose:* Enables behavioral analysis and loyalty tracking. It answers: *"How many returning customers do we have, and where do they live today?"*
*   **`dim_date`**: The **Temporal Skeleton**.
    *   *Purpose:* Essential for time-series analysis. It allows the business to compare "Saturdays vs Sundays" or "This Christmas vs Last Christmas" without complex SQL date manipulation.

### 3. Slowly Changing Dimensions (SCD Type 2)
One of the most advanced features in the Gold layer is the implementation of **SCD Type 2** for `dim_customer`. This allows the business to track historical changes over time (e.g., if a customer moves from London to New York, we preserve their history in both cities).

*   **Implementation Mechanics**:
    *   **Watermarking**: The script retrieves the `max(valid_from)` from the existing Gold table to only process new changes since the last run.
    *   **Versioning**: Every customer record has a `valid_from`, `valid_to`, and an `is_current` flag.
    *   **The "Close & Open" Workflow**: When a change is detected:
        1.  The existing record is "closed" by setting `is_current = false` and `valid_to = now()`.
        2.  A new record is "opened" with the updated details and `is_current = true`.
*   **Why it's better**: Without SCD Type 2, a customer's old sales would incorrectly look like they happened in their new city. SCD Type 2 ensures 100% historical accuracy.

### 3. Gold Layer "Logic List" (Business Modeling)
The Gold layer isn't just cleaning; it's applying business intelligence rules.

| Logic Type | Implementation Mechanic | Business Purpose |
| :--- | :--- | :--- |
| **Silo Unification** | `UNION` of Retail, POS, and Warehouse | Creates a single "Source of Truth" for all Products and Customers across the enterprise. |
| **Date Spining** | `dim_date` Table Generation | Creates a continuous timeline, allowing for "Day-over-Day" or "Weekend vs Weekday" sales analysis even on days with no sales. |
| **KPI Derivation** | `datediff('day', ship, delivery)` | Calculates operational metrics like "Average Delivery Time" which don't exist in the raw data. |
| **Integrity Joins** | `Sales JOIN Dim_Prod` | Replaces raw `product_id` with optimized `product_key`, ensuring every sale points to a valid product definition. |
| **Unknown Handling** | `COALESCE` with System Key `0` | Ensures that sales with missing customer IDs are still trackable under a "General/Unknown" bucket rather than being lost. |

### 4. Performance & Storage Optimization
Because Gold tables are queried directly by the Dashboard, we apply aggressive performance optimizations during every merge:

*   **File Compaction**: The script calls `dt.optimize().execute_compaction()`. This merges the many small Parquet files created by frequent updates into larger, more efficient files, reducing query latency.
*   **Checkpointing**: `dt.create_checkpoint()` is used to collapse the Delta transaction log. This speeds up the "metadata read" time for the API and Dashboard.
*   **Predicate Pruning**: Fact tables (like `fact_sales`) are partitioned by `year`, `month`, and `day`. The Star Schema joins are designed to leverage these partitions to skip scanning unnecessary data.

### 5. How Delta Lake Ensures ACID in Gold
Just like Silver, Gold uses the **Delta Log** to ensure that an executive looking at a dashboard never sees partial or corrupted data.
*   **Atomic Merges**: The complex SCD Type 2 logic (multiple updates and inserts) happens within a single Delta transaction. It either all commits or none of it does.
*   **Snapshot Consistency**: Even if a multi-minute "Optimize/Compaction" job is running, the Dashboard continues to read from the last stable snapshot without performance degradation or locks.
*   **Metadata Checkpoints**: By using `.create_checkpoint()`, we ensure that the system can quickly determine the current state of the 599-line `gold_layer.py` output without replaying 1,000s of individual JSON files.

## Phase 3: Consumption & Visualization

The final stage of the Medallion architecture turns processed data into competitive advantage. This phase covers the **Analytics API** (the bridge) and the **Retail Dashboard** (the interface).

### 1. The Analytics API (`api/main.py`)
Built with **FastAPI**, this server acts as the high-performance interface between the Delta Lakehouse and the Frontend.

*   **Persistent Connection & Views**:
    *   The API initializes a single, shared **DuckDB connection** at startup.
    *   It creates **DuckDB Views** on top of the Delta tables using `delta_scan`. This caches the table metadata, ensuring that every API request doesn't have to re-read the Delta log from scratch.
*   **Thread-Safe Concurrency**:
    *   Analytical queries can be CPU-intensive. The API uses a `threading.Lock()` to ensure that concurrent requests don't interfere with the internal state of the DuckDB connection, providing a stable experience for multiple dashboard users.
*   **Result Caching**:
    *   To ensure "Sub-Second" responsiveness, the API implements an **in-memory cache**. Frequently accessed KPIs (like Total Revenue) are served instantly without hitting the disk.
*   **Admin Sync hooks**:
    *   The API exposes a `/api/admin/refresh` endpoint. This is triggered by the **Master Orchestration Engine** (`main.py`) immediately after a Gold Layer run to clear the cache and refresh the metadata views, ensuring the dashboard is always displaying the freshest data.

### 2. The Retail Dashboard (`ui/`)
The Frontend is a modern, responsive **React** application built for clarity and impact.

*   **Modular Architecture**:
    *   The UI is divided into logical "Slices": **Commercial**, **Operations**, **Customer**, and **Lineage**. This reflects the Star Schema design of the Gold Layer.
*   **High-Impact Visuals**:
    *   **Chart.js Integration**: Uses hardware-accelerated canvas rendering to display complex revenue trends and distribution charts.
    *   **Theming Engine**: Supports a "Glassmorphism" aesthetic with both Light and Dark modes, catering to both office and warehouse environment visibility.
*   **Data Lineage Visualizer**:
    *   A custom-built "Funnel" component that reveals the inner workings of the pipeline. It pulls live row counts from **Landing $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold**, giving users confidence that the data they see is accurate and fully processed.
*   **Interactive Controls**:
    *   The **"Sync Data"** button allows users to bridge the gap between "Live Simulation" and "Batch Processing," triggering a full Medallion pipeline run directly from the browser.
