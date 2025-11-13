import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import os
import matplotlib.pyplot as plt

def read_csv(filename):
    data = pd.read_csv(filename)
    
    data["ingested_at"] = datetime.now()
    data["source_file"] = os.path.basename(filename)
    return data

customers = read_csv("../datasets/customers.csv")
inventory = read_csv("../datasets/inventory.csv")
order_items = read_csv("../datasets/order_items.csv")
orders = read_csv("../datasets/orders.csv")
products = read_csv("../datasets/products.csv")
refunds = read_csv("../datasets/refunds.csv")

#apply deduplication
customers = customers.sort_values("ingested_at").drop_duplicates(subset=["customer_id"], keep="last")

orders = orders.sort_values("ingested_at").drop_duplicates(subset=["order_id"], keep="last")

order_items = order_items.sort_values("ingested_at").drop_duplicates(subset=['order_id'], keep='last')

products = products.sort_values("ingested_at").drop_duplicates(subset=["product_id"], keep="last")

inventory = inventory.sort_values("ingested_at").drop_duplicates(subset=["product_id", "received_date"], keep="last")

refunds = refunds.sort_values("ingested_at").drop_duplicates(subset=["order_id", "refund_date"], keep="last")

#data quality checks
quarantine = pd.DataFrame()

# Age check
invalid_age = customers[~customers['age'].between(18, 100)].copy()
invalid_age["error_type"] = "Invalid Age"
quarantine = pd.concat([quarantine, invalid_age])

# Gender check
valid_genders = ["Male", "Female", "Other"]
invalid_gender = customers[~customers['gender'].isin(valid_genders)].copy()
invalid_gender["error_type"] = "Invalid Gender"
quarantine = pd.concat([quarantine, invalid_gender])

#SCD-2
products = products.sort_values(['product_id', 'ingested_at'])
scd2_products = []
current_records = {}

for _ , row in products.iterrows():
    pid = row['product_id']
    price = row['price']
    date = row['ingested_at']

    if pid not in current_records:
        current_records[pid] = {
            "product_id": pid,
            "price": price,
            "start_date": date,
            "end_date": None
        }
    else:
        if current_records[pid]['price'] != price:
            current_records[pid]['end_date'] = date

            scd2_products.append(current_records[pid])

            current_records[pid] = {
                "product_id": pid,
                "price": price,
                "start_date": date,
                "end_date": None
            }

scd2_products.extend(current_records.values())

dim_scd2_products = pd.DataFrame(scd2_products)

#converting signup date from object to datetime and country as category
customers['signup_date'] = pd.to_datetime(customers['signup_date'])
customers['country'] = customers['country'].str.capitalize().str.strip()
customers['country'] = customers['country'].astype("category")

#standardize text columns 0f cutomers data
customers['gender'] = customers['gender'].str.capitalize().str.strip()
customers['region'] = customers['region'].str.capitalize().str.strip()


#Handling null values customers data
avg_age = customers['age'].mean()

customers['age'] = customers['age'].fillna(avg_age)

#converting received date from object to datetime
inventory['received_date'] = pd.to_datetime(inventory['received_date'])

#converting order date from object to datetime
orders['order_date'] = pd.to_datetime(orders['order_date'])

#standardize text columns 0f orders data
orders['channel'] = orders['channel'].str.capitalize().str.strip()
orders['currency'] = orders['currency'].str.upper().str.strip()
orders['coupon_code'] = orders['coupon_code'].str.capitalize().str.strip()
orders['order_status'] = orders['order_status'].str.capitalize().str.strip()

#Handling null values of orders data
orders['coupon_code'] = orders['coupon_code'].fillna('No discount')

#standardize text columns 0f products data
products['brand'] = products['brand'].str.upper().str.strip()
products['category'] = products['category'].str.capitalize().str.strip()
products['subcategory'] = products['subcategory'].str.capitalize().str.strip()

#coverting the data type of category to category
products['category'] = products['category'].astype('category')

#standardize text columns 0f refunds data
refunds['reason'] = refunds['reason'].str.capitalize().str.strip()

#coverting the data type of refund_date from object to datetime
refunds['refund_date'] = pd.to_datetime(refunds['refund_date'])

#dim_customer
dim_customer = customers[['customer_id','signup_date','country','region','age','gender']]

#dim_product
dim_product = products[["product_id",'category','subcategory','brand','price','cost']]

#dim_date
dim_date = pd.DataFrame({
    "date_id": orders['order_date'].dt.strftime("%Y%m%d"),
    "date": orders['order_date'],
    "year": orders['order_date'].dt.year,
    "month": orders['order_date'].dt.month,
    "day": orders['order_date'].dt.day
})

# fct_orders
fct_orders = orders[["order_id", "customer_id", "order_date", "order_status", "coupon_code"]].copy()
fct_orders["date_id"] = fct_orders["order_date"].dt.strftime("%Y%m%d")

# fct_order_items
fct_order_items = order_items[["order_id", "product_id", "quantity", "unit_price", "discount"]].copy()
fct_order_items["gross_amount"] = fct_order_items["quantity"] * fct_order_items["unit_price"]
fct_order_items["net_amount"] = fct_order_items["gross_amount"] * (1 - fct_order_items["discount"])

# fct_returns
refunds["refund_date"] = pd.to_datetime(refunds["refund_date"])
refunds = refunds.merge(order_items, on="order_id", how="left")
fct_returns = refunds[["order_id", "product_id_x", "refund_date", "reason", "quantity", "unit_price", "discount"]].copy()
fct_returns["refund_amount"] = fct_returns["quantity"] * fct_returns["unit_price"] * (1 - fct_returns["discount"])

#fct inventory
inventory["received_date"] = pd.to_datetime(inventory["received_date"])
fct_inventory_daily = inventory[["product_id", "on_hand", "reorder_point", "received_date"]].copy()
fct_inventory_daily.rename(columns={"received_date": "date"}, inplace=True)

#cohort_ltv
first_purchase_date = orders.groupby('customer_id')['order_date'].min().reset_index()
first_purchase_date = first_purchase_date.rename(columns={"order_date":"first_purchase_date"})

orders = orders.merge(first_purchase_date, on='customer_id', how='left')

order_items['revenue'] = order_items['quantity'] * order_items['unit_price']
orders = pd.merge(orders, order_items[['order_id', 'revenue']], on='order_id', how='left')

orders['days_since_first_purchase'] = (orders['order_date'] - orders['first_purchase_date']).dt.days
windows = [7, 30, 90]

fct_cohort_ltv = pd.DataFrame({"cohort_date": first_purchase_date["first_purchase_date"].unique()})

for w in windows:
    cohort_revenue = (orders[orders["days_since_first_purchase"] <= w]
                      .groupby("first_purchase_date")["revenue"].sum())
    
    cohort_customers = first_purchase_date.groupby("first_purchase_date")["customer_id"].nunique()    
    cohort_ltv = (cohort_revenue / cohort_customers).rename(f"LTV_{w}_days")
    
    fct_cohort_ltv = fct_cohort_ltv.merge(cohort_ltv, left_on="cohort_date", right_index=True, how="left")

#calculating gross revenue
gross_revenue = order_items['quantity'] * order_items['unit_price'] 
gross_revenue = gross_revenue.sum()

#calculating Average order value(AOV)
total_orders = order_items['order_id'].nunique()
Avg_o_v = gross_revenue / total_orders

#by month
merge_by_date = order_items.merge(orders, on='order_id', how='left')
merge_by_date['revenue'] = merge_by_date['quantity'] * merge_by_date['unit_price']
merge_by_date['order_month'] = merge_by_date['order_date'].dt.to_period('M')

monthly_AOV = merge_by_date.groupby('order_month')['revenue'].sum()
unique_orders = merge_by_date.groupby('order_month')['order_id'].nunique()
monthly_AOV = monthly_AOV/unique_orders


#checking new customers or repeat
valid_orders = orders[orders['order_status'] == 'Shipped'].copy()

valid_orders = valid_orders.sort_values(by=['customer_id','order_date'])
valid_orders['customer_type'] = valid_orders.duplicated(subset=['customer_id']).map({True:'Repeat',False:'New'})

orders = pd.merge(orders, valid_orders[['order_id','customer_type']], on='order_id', how='left')
orders['customer_type'] = orders['customer_type'].fillna(0)
    
#checking Customer retention
retention = orders[['customer_id','order_date','customer_type']].copy()

retention['month'] = retention['order_date'].dt.to_period('M')
monthly_customers = retention.groupby(['month', 'customer_type'])['customer_id'].nunique()
monthly_customers = monthly_customers.unstack()

new = monthly_customers['New']
repeat = monthly_customers['Repeat']
total = new + repeat
prev_month_customers = total.shift(1)

#formula for CRR(customer retention rate)
CRR = ((total-new)/prev_month_customers) * 100

#Calculating LTV
unique_customers = orders['customer_id'].nunique()
purchase_frequency = total_orders/unique_customers

customer_life_span = orders.groupby('customer_id')['order_date'].agg(['min', 'max'])
customer_life_span['months_active'] = (customer_life_span['max'] - customer_life_span['min']).dt.days/30
avg_life_span = customer_life_span['months_active'].mean()

#lifetime value of a customer
LTV = Avg_o_v * purchase_frequency * avg_life_span

#inventory Alerts
check_alerts = inventory.groupby('product_id')[['on_hand', 'reorder_point']].sum()

check_alerts['alert_status'] = np.where(check_alerts['on_hand'] <= check_alerts['reorder_point'], 'At_risk', 'Available')

#checking for discounts
check_discounts = pd.merge(order_items, orders[['order_id','coupon_code']], on='order_id', how='left')

no_discount = check_discounts[check_discounts['coupon_code'] == 'No discount']
no_discount_revenue = (no_discount['quantity'] * no_discount['unit_price']).sum()

Discount20 = check_discounts[check_discounts['coupon_code'] == 'Discount20']
Discount20_revenue = (Discount20['quantity'] * Discount20['unit_price'] * 0.8).sum()

Save10 = check_discounts[check_discounts['coupon_code'] == 'Save10']
Save10_revenue = (Save10['quantity'] * Save10['unit_price'] * 0.9).sum()

Welcome = check_discounts[check_discounts['coupon_code'] == 'Welcome']
Welcome_revenue = (Welcome['quantity'] * Welcome['unit_price']).sum()

Summer = check_discounts[check_discounts['coupon_code'] == 'Summer']
Summer_revenue = (Summer['quantity'] * Summer['unit_price'] * 0.7).sum()


#calculating discount  from discount column
discount = (order_items['quantity'] * order_items['unit_price'] * order_items['discount'])

#refunds
refunds = refunds.merge(orders[['order_id', 'order_date']], on='order_id', how='left')
print(refunds.columns)
refunds['line_amount'] = refunds['quantity'] * refunds['unit_price']
refunds['line_discount'] = refunds['quantity'] * refunds['unit_price'] * refunds['discount']
refunds['net_line_amount'] = refunds['line_amount'] - refunds['line_discount']

refunds['days_diff'] = (refunds['refund_date'] - refunds['order_date']).dt.days
refunds['refund_price'] = np.where(refunds['days_diff'] <= 30, refunds['net_line_amount'], 0)

#net_revenue
discount = discount.sum()
net_line_amount = refunds['net_line_amount'].sum()
net_revenue = gross_revenue - discount - net_line_amount

#daily orders
daily_orders = merge_by_date.groupby('order_date')['order_id'].nunique()

#daily customers
daily_customers = orders.groupby('order_date')['customer_id'].nunique()

#daily revenut
daily_revenue = merge_by_date.groupby('order_date')['revenue'].sum()

#Revenue Trend line
# Daily revenue trend
# Monthly revenue trend
monthly_revenue = merge_by_date.groupby('order_month')['revenue'].sum()

plt.figure(figsize=(10,6))
plt.plot(monthly_revenue.index.astype(str), monthly_revenue.values, marker='o', linestyle='-', color='green')
plt.title("Monthly Revenue Trend")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# Retention curve
monthly_customers = retention.groupby(['month', 'customer_type'])['customer_id'].nunique().unstack()

plt.figure(figsize=(10,6))
for ctype in monthly_customers.columns:
    plt.plot(monthly_customers.index.astype(str), monthly_customers[ctype], marker='o', label=ctype)

plt.title("Customer Retention Curve (New vs Repeat)")
plt.xlabel("Month")
plt.ylabel("Number of Customers")
plt.xticks(rotation=45)
plt.legend()
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()


# Inventory alert bar chart
alert_counts = check_alerts['alert_status'].value_counts()

plt.figure(figsize=(8,5))
alert_counts.plot(kind='bar', color=['red','green'])
plt.title("Inventory Alert Status")
plt.xlabel("Status")
plt.ylabel("Number of Products")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()
