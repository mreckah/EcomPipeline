#!/usr/bin/env python3
import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import seaborn as sns

# ---------------------- PostgreSQL config ----------------------
PG_USER = "warehouse_user"
PG_PASS = "warehouse_pass"
PG_HOST = "localhost"
PG_PORT = "5433"
PG_DB   = "warehouse"
TABLE_INPUT  = "sales_data"
TABLE_OUTPUT = "sales_with_predictions"

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# Ensure plot folder exists
os.makedirs("./plots", exist_ok=True)

# ---------------------- Load dataset ----------------------
print("V Loading data from PostgreSQL...")
df = pd.read_sql(f"SELECT * FROM {TABLE_INPUT}", con=engine)
print(f"V Loaded {len(df)} rows")

# ---------------------- Clean dataset ----------------------
print("V Cleaning data...")

# Check what columns actually exist (case-insensitive)
df_columns_lower = {col.lower(): col for col in df.columns}
required_cols = {}

# Map required columns to actual column names
for req_col in ["Sales", "Profit", "Quantity", "Discount"]:
    if req_col.lower() in df_columns_lower:
        required_cols[req_col] = df_columns_lower[req_col.lower()]
    else:
        print(f"⚠️  Warning: Column '{req_col}' not found in dataset. Available columns: {list(df.columns)}")
        # Try to find similar column names
        similar = [c for c in df.columns if req_col.lower() in c.lower() or c.lower() in req_col.lower()]
        if similar:
            print(f"   Found similar columns: {similar}")

# Clean numeric columns
for col_name, actual_col in required_cols.items():
    if col_name in ["Sales", "Profit"]:
        df[actual_col] = df[actual_col].replace(r'[\$,()]', '', regex=True).astype(float)
    elif col_name == "Discount":
        df[actual_col] = pd.to_numeric(df[actual_col].astype(str).str.replace("%",""), errors="coerce") / 100
    elif col_name == "Quantity":
        df[actual_col] = pd.to_numeric(df[actual_col], errors="coerce")

# Use actual column names for features
features = [required_cols.get("Sales"), required_cols.get("Quantity"), required_cols.get("Discount")]
features = [f for f in features if f is not None]  # Remove None values
profit_col = required_cols.get("Profit")

if not features or profit_col is None:
    print(f"❌ ERROR: Missing required columns. Need: Sales, Quantity, Discount, Profit")
    print(f"   Available columns: {list(df.columns)}")
    exit(1)

# Drop rows with missing values in required columns
df = df.dropna(subset=features + [profit_col])
print(f"V Dataset cleaned, {len(df)} rows ready for modeling")

# ---------------------- Anomaly detection ----------------------
print("V Detecting anomalies with Isolation Forest...")
iso = IsolationForest(contamination=0.05, random_state=42)
df['anomaly'] = iso.fit_predict(df[features])
num_anomalies = sum(df['anomaly'] == -1)
print(f"V Found {num_anomalies} anomalies out of {len(df)} rows")

# ---------------------- Train regression ----------------------
df_clean = df[df['anomaly'] != -1].copy()
X_train = df_clean[features]
y_train = df_clean[profit_col]

print("V Training Linear Regression model...")
model = LinearRegression()
model.fit(X_train, y_train)
df_clean["Predicted_Profit"] = model.predict(X_train)

rmse = mean_squared_error(y_train, df_clean["Predicted_Profit"], squared=False)
print(f"V Test RMSE: {rmse:.2f}")

# ---------------------- Save predictions ----------------------
print(f"V Saving predictions to PostgreSQL table '{TABLE_OUTPUT}'...")
df_clean.to_sql(TABLE_OUTPUT, con=engine, if_exists="replace", index=False)
print("V Predictions saved successfully")

# ---------------------- Visualization ----------------------
print("V Generating plots...")

# Sales vs Profit
sales_col = required_cols.get("Sales")
discount_col = required_cols.get("Discount")

if sales_col and profit_col:
    plt.figure(figsize=(10,6))
    sns.scatterplot(
        data=df, x=sales_col, y=profit_col,
        hue=df['anomaly'].map({1:'Normal', -1:'Anomaly'}),
        palette={'Normal':'blue','Anomaly':'red'},
        s=80
    )
    plt.title("V Sales vs Profit (Red = Anomaly)")
    plt.xlabel("Sales")
    plt.ylabel("Profit")
    plt.tight_layout()
    plt.savefig("./plots/sales_vs_profit.png")
    plt.close()

# Profit vs Discount
if discount_col and profit_col:
    plt.figure(figsize=(10,6))
    sns.scatterplot(
        data=df, x=discount_col, y=profit_col,
        hue=df['anomaly'].map({1:'Normal', -1:'Anomaly'}),
        palette={'Normal':'blue','Anomaly':'red'},
        s=80
    )
    plt.title("V Discount vs Profit (Red = Anomaly)")
    plt.xlabel("Discount")
    plt.ylabel("Profit")
    plt.tight_layout()
    plt.savefig("./plots/discount_vs_profit.png")
    plt.close()

print("V Plots saved in './plots/' folder")
