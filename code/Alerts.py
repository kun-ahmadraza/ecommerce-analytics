from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import numpy as np
import e_commerce_anyltics
from datetime import datetime

app = FastAPI()

templates = Jinja2Templates(directory='../templates')

@app.get("/", response_class=HTMLResponse)
async def daily_report(request:Request, start_date: str= Query(None), end_date: str= Query(None)):
    df = pd.DataFrame({
        "date": e_commerce_anyltics.daily_revenue.index,
        "revenue": e_commerce_anyltics.daily_revenue.values,
        "customers": e_commerce_anyltics.daily_customers.values,
        "orders": e_commerce_anyltics.daily_orders.values
    })
    
    df['date'] = pd.to_datetime(df["date"])
    if start_date and end_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]

    df['date'] = df['date'].dt.strftime("%y-%m-%d")
    df = df.sort_values(by='date')

    return templates.TemplateResponse('daily_report.html', {"request":request, "columns":df.columns, "rows":df.to_dict(orient='records'), "start_date": start_date, "end_date":end_date})

@app.get("/ltv_by_days")
async def ltv_by_days(request: Request):
    ltv = e_commerce_anyltics.fct_cohort_ltv
    ltv['cohort_date'] = pd.to_datetime(ltv['cohort_date'])
    ltv["cohort_date"] = ltv["cohort_date"].dt.strftime("%Y-%m-%d")
    ltv = ltv.sort_values(by='cohort_date')
    return templates.TemplateResponse("ltv_by_days.html", {"request": request, "ltv_columns": ltv.columns, "ltv_rows": ltv.to_dict(orient='records')})

@app.get("/inventory_alerts")
async def inventory_alerts(request:Request):
    inventory = pd.DataFrame(e_commerce_anyltics.inventory)
    
    check_alerts = inventory.groupby('product_id')[['on_hand', 'reorder_point']].sum()

    check_alerts['alert_status'] = np.where(check_alerts['on_hand'] <= check_alerts['reorder_point'], 'At_risk', 'Available')
    df = pd.DataFrame({
        "product_id":check_alerts.index,
        "on_hand":check_alerts['on_hand'].values,
        "reorder_point":check_alerts['reorder_point'].values,
        "alert_status":check_alerts['alert_status'].values})

    return templates.TemplateResponse('inventory_alerts.html', {"request":request, "in_columns": df.columns, "in_rows": df.to_dict(orient='records')})