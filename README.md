# Ecommerce-analytics

## Overview  
This project is about creating a **realistic analytics platform** for an online store.  
We build a clean data model, reliable ETL pipelines, and an API to serve business metrics.  
The goal is to simulate real Data Engineering + Data Science workflows in Python.  

---

## Business Context  
A Direct-to-Consumer (D2C) store sells products to customers.  
The business needs **daily insights** to make decisions about:  
-  Revenue (gross & net)  
-  Orders and returns  
-  New vs. repeat customers  
-  Average order value (AOV)  
-  Customer retention & lifetime value (LTV)  
-  Inventory stockout alerts  

---

## Key KPIs  
- Gross & Net Revenue  
- Average Order Value (AOV)  
- Return Rate  
- New vs. Repeat Customers  
- Customer Retention (7/30/90 days)  
- LTV Cohorts (7/30/90 days)  
- Inventory Alerts  

---

## Tech Stack  
- **Python** (Pandas, NumPy)  
- **Matplotlib** for reports  
- **FastAPI + Pydantic** for APIs  
- **PostgreSQL** as warehouse  
- **Multiprocessing / Asyncio** for orchestration  

---

## Data Model  
- **Dimensions**:  
  - `dim_customer`  
  - `dim_product` (SCD-2 for price history)  
  - `dim_date`  

- **Facts**:  
  - `fct_orders`  
  - `fct_order_items`  
  - `fct_inventory_daily`  
  - `fct_returns`  
  - `fct_revenue_daily`  
  - `fct_cohort_ltv`  

---

## Business Rules  
- Order flow: `created → paid → fulfilled → delivered → returned?`  
- **Net Revenue** = gross - discounts - refunds  
- Refund only if returned ≤ 30 days  
- Inventory alert: `on_hand - allocated < reorder_point`  
- Deduplication: keep latest order by `ingested_at`  
- Track product price changes (SCD-2)  

---

## Deliverables  
- **ETL Pipelines** (staging → dims/facts)  
- **Data Quality Checks** (null %, ranges, duplicates)  
- **Quarantine Table** for bad records  
- **FastAPI Endpoints**  
  - `/metrics/daily`  
  - `/ltv/{days}`  
  - `/inventory/alerts`  
- **Reports**: revenue trends, AOV, retention, inventory alerts  

---

## Definition of Done  
- Idempotent ETL (no duplicates)  
- Net Revenue matches business formula  
- Retention & LTV computed for cohorts  
- Inventory alerts tested on ≥ 5 SKUs  
- Unit tests > 90% coverage  
- API responses validated with Pydantic  


