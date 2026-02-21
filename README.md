# RetailSink

**Real-Time Retail Analytics Platform** built on the Medallion Architecture.

RetailSink is an end-to-end data platform that simulates retail operations (E-commerce, POS, Warehouse) and processes the resulting data through a multi-layered Lakehouse architecture to provide real-time business insights.

---

## 📚 Documentation Guide

This project includes several detailed markdown files covering different aspects of the architecture and implementation:

| File | Purpose |
| :--- | :--- |
| [documentation.md](documentation.md) | **Main Technical Docs**: Detailed overview of the pipeline scripts, design choices (ELT, DuckDB, Delta Lake), and SCD Type 2 implementation. |
| [data_flow.md](data_flow.md) | **Architecture Visualization**: Mermaid diagrams and descriptions of data movement from Landing to Gold layers. |
| [notes.md](notes.md) | **Developer Notes**: Data schema mappings, simulator fault probabilities, and internal phase-wise planning. |
| [PowerBI_Demo_Video.md](PowerBI_Demo_Video.md) | **Product Demo**: External drive link to a video demonstrating the PowerBI dashboard implementation. |

---

## 🚀 Quick Start

To run the entire RetailSink platform locally:

### 1. Start the Core Engine (Simulators & API)
Run the master orchestrator to start data generation and the backend server:
```bash
python main.py
```

### 2. Start the Dashboard UI
In a new terminal, navigate to the UI directory and start the Vite dev server:
```bash
cd ui
npm run dev
```

The dashboard will be available at the URL displayed in your terminal.

### 3. API Documentation
The analytical backend runs in the background. You can access the interactive API docs at the `/docs` endpoint of the API server.
