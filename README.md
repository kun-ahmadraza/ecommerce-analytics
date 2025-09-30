# ecommerce-analytics
Overview
You’re contracted to deliver three progressively challenging, real‑world projects that exercise Data Engineering and Data Science skills using Python, NumPy, Pandas, Matplotlib, OOP, Threading/Multiprocessing, Asyncio, FastAPI, and Pydantic. Each project includes:
Clear business context & KPIs
Explicit business rules
Data model and pipeline specs
Tech stack requirements (open‑source only)
Step‑by‑step tasks & milestones
Acceptance criteria (“Definition of Done”)
What to include in your progress updates
Where to get data / how to generate it if needed
You can complete them in order (recommended) or run in parallel. I am the owner of p this product. Share weekly updates and short demos when major milestones are hit.

Project 1 — E‑Commerce Analytics Platform (Batch ETL ➝ Warehouse ➝ API)
Goal: Build a small but realistic analytics platform for an online store. Deliver a clean data model, reliable batch pipelines, reproducible transformations, and a FastAPI service for downstream consumers (BI tools or partners).
Business Context
A D2C store sells products (SKUs) to customers. We need trustworthy daily metrics for revenue, orders, returns, inventory status, and customer retention (LTV & cohorts). Finance also needs GAAP‑style revenue recognition for returns/refunds.
KPIs
Daily Gross Revenue, Net Revenue
Average Order Value (AOV)
Return Rate
New vs. Repeat Customers
7/30/90‑day Customer Retention & LTV
Inventory Stockout Alerts (simple threshold)
Business Rules (authoritative)
Order states: created → paid → fulfilled → delivered → returned? (not all states always occur).
Net Revenue: gross_revenue - discounts - refunds - taxes (taxes are excluded from revenue).
Refunds: If an order is returned within 30 days of delivery, refund its net line amount to the original payment method.
LTV windowing: Compute cohort‑based LTV at 7/30/90 days using first purchase date as cohort anchor.
Inventory: A SKU is "at risk" if on_hand - allocated < reorder_point. Fire an alert record.
Deduplication: Orders are uniquely identified by order_id; later CSVs may include re‑sent rows—keep the latest record by ingested_at.
Slow‑changing dimensions: Track product price history (SCD‑2) when list_price changes.
Data Sources
Choose one path (both acceptable):
Open dataset (quick start): Use the synthetic “RetailRocket” or “Online Retail II” datasets (Kaggle/UCT). If access is inconvenient, use the Fabricated Data Generator below.
Fabricated Data Generator: Implement a Python data generator (use faker) to output CSVs for orders, order_items, customers, products, inventory_movements with realistic distributions and noise.
Target Data Model (Warehouse)
Dimension tables: dim_customer, dim_product (SCD‑2), dim_date.
Fact tables: fct_orders, fct_order_items, fct_inventory_daily, fct_returns, fct_revenue_daily, fct_cohort_ltv.
Staging (raw): Mirror incoming CSVs/JSON as stg_* with ingestion metadata.
Architecture & Tech Stack (constraints)
Storage/DB: PostgreSQL (primary) + local parquet in a /data/ lake‑like folder for staging/intermediate files.
Orchestration: Start with Python CLI + cron/Windows Task Scheduler or a simple asyncio runner. (If you know Airflow later, add it as a stretch.)
Compute: Python; use Pandas for transforms; use multiprocessing for partitioned loads; use threading/asyncio for I/O (e.g., reading many CSVs).
APIs: FastAPI with Pydantic models to serve: /metrics/daily, /ltv/{days}, /inventory/alerts.
Validation: pydantic schemas for input rows + custom checks (nulls, ranges, enums). Reject bad records to a quarantine table with reasons.
Visualization: Matplotlib charts for a lightweight report.
Tasks & Milestones
Data Contracts & Schemas (Pydantic models for each file/topic).
Ingestion Layer
File watcher or batch pickup from /landing/{date}/.
Idempotent load to stg_* with ingested_at, source_file, row_hash.
Transformations
Build dim_* and fct_* tables; implement SCD‑2 for product price.
Revenue and returns logic per business rules.
Data Quality
Great Expectations‑like checks (you can hand‑roll): null %, unique keys, referential integrity, value ranges.
Quarantine bad records; daily summary report.
API Layer (FastAPI)
Endpoints documented with OpenAPI; responses validated with Pydantic.
Analytics & Reporting
Produce PNG/HTML reports: time series of net revenue, AOV, retention curves, inventory alerts.
Packaging & CI
Makefile/CLI; unit tests for transformations and business rules.
Deliverables
/docs/ system design (diagram + explanation), data dictionary, business rules.
/orchestration/ runner (asyncio) and job definitions.
/pipelines/ ETL scripts with tests.
/warehouse/ DDL (Postgres) + migration scripts.
/api/ FastAPI app with Pydantic schemas and example curl requests.
/reports/ charts and a one‑page “Insights” memo.
Acceptance Criteria (DoD)
Re‑runs are idempotent; duplicates do not appear in facts/dims.
“Net Revenue” matches the defined formula across 10 random orders.
LTV and retention computed for at least 3 cohorts and exposed via API.
Inventory alerts produced with correct thresholds for at least 5 SKUs.
90%+ unit test coverage on transformation utilities and business rules.
What to Include in Your Updates
Run log: volumes, durations, row counts.
Data quality results (pass/fail, quarantined rows).
Screenshots of Matplotlib charts + sample API responses.

