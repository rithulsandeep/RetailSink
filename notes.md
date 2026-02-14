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
