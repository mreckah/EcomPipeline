#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import IsolationForest
from xgboost import XGBRegressor, plot_importance
import matplotlib.pyplot as plt
import joblib

# ---------------------- Config ----------------------
PG_USER = "warehouse_user"
PG_PASS = "warehouse_pass"
PG_HOST = "localhost"
PG_PORT = "5433"
PG_DB = "warehouse"
TABLE_INPUT  = "sales_data"
TABLE_OUTPUT = "sales_with_predictions"

MODEL_DIR = "./models"
PLOTS_DIR = "./plots"
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(PLOTS_DIR, exist_ok=True)

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# ---------------------- Helpers ----------------------
def parse_number_mixed(s):
    if pd.isna(s):
        return np.nan
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).replace("$","").replace("(","").replace(")","").replace(" ","")
    if '.' in s and ',' in s and s.rfind(',') > s.rfind('.'):
        s = s.replace('.','').replace(',','.')
    elif ',' in s and s.count(',') == 1 and len(s.split(',')[1]) <= 3:
        s = s.replace(',','.')
    else:
        s = s.replace(',','')
    try:
        return float(s)
    except:
        return np.nan

def parse_discount(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x,(int,float)):
        if 0 < x <= 1:
            return x
        elif 1 < x <= 100:
            return x/100
        elif x==0:
            return 0
        else:
            return x
    s = str(x).replace("%","").strip()
    try:
        val = float(s)
        if 0 < val <= 1:
            return val
        elif 1 < val <= 100:
            return val/100
        elif val==0:
            return 0
        else:
            return val
    except:
        return np.nan

# ---------------------- Load dataset ----------------------
print("✓ Loading data from PostgreSQL...")
df = pd.read_sql(f"SELECT * FROM {TABLE_INPUT}", con=engine)
print(f"✓ Loaded {len(df)} rows")

# ---------------------- Clean numeric columns ----------------------
df["Sales"] = df["Sales"].apply(parse_number_mixed)
df["Profit"] = df["Profit"].apply(parse_number_mixed)
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df["Discount"] = df["Discount"].apply(parse_discount)
df["Order_Month"] = pd.to_datetime(df["Order Date"], errors="coerce").dt.month

df = df.dropna(subset=["Sales","Profit","Quantity","Discount","Order_Month"])
df = df[df["Sales"] > 0]
print(f"✓ Cleaned dataset: {len(df)} rows")

# ---------------------- Encode categoricals ----------------------
categoricals = ["Ship Mode","Segment","Category","Sub-Category","Region"]
encoders = {}
for col in categoricals:
    le = LabelEncoder()
    df[col+"_Encoded"] = le.fit_transform(df[col].astype(str))
    encoders[col] = le

# ---------------------- Features ----------------------
features = [
    "Sales","Quantity","Discount","Order_Month",
    "Ship Mode_Encoded","Segment_Encoded","Category_Encoded","Sub-Category_Encoded","Region_Encoded"
]

df = df.dropna(subset=features)

# ---------------------- Optional anomaly removal ----------------------
iso = IsolationForest(contamination=0.03, random_state=42)
df['anomaly'] = iso.fit_predict(df[features])
df_clean = df[df['anomaly'] != -1].copy()
print(f"✓ Clean rows after anomaly removal: {len(df_clean)}")

# ---------------------- Train/Test split ----------------------
X = df_clean[features]
y = df_clean["Profit"]
X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2,random_state=42)

# ---------------------- Hyperparameter tuning & training ----------------------
param_grid = {
    "n_estimators": [300,500,700],
    "max_depth":[3,5,7],
    "learning_rate":[0.01,0.05,0.1],
    "subsample":[0.6,0.8,1.0],
    "colsample_bytree":[0.6,0.8,1.0]
}

xgb = XGBRegressor(random_state=42,n_jobs=-1,objective="reg:squarederror")
search = RandomizedSearchCV(xgb,param_distributions=param_grid,n_iter=20,scoring="r2",cv=3,verbose=1,random_state=42)
search.fit(X_train,y_train)
best_model = search.best_estimator_
print("✓ Best parameters:", search.best_params_)

# ---------------------- Evaluation ----------------------
y_pred_test = best_model.predict(X_test)
rmse_test = mean_squared_error(y_test,y_pred_test,squared=False)
r2_test   = r2_score(y_test,y_pred_test)
print(f"✓ Test RMSE: {rmse_test:.2f}")
print(f"✓ Test R²: {r2_test:.3f}")

# ---------------------- Save model & metadata ----------------------
joblib.dump(best_model, os.path.join(MODEL_DIR,"xgb_model.pkl"))
joblib.dump(encoders, os.path.join(MODEL_DIR,"encoders.pkl"))
joblib.dump(features, os.path.join(MODEL_DIR,"feature_list.pkl"))
print("✓ Model, encoders, features saved")

# ---------------------- Save predictions ----------------------
df_test = X_test.copy()
df_test["Actual_Profit"] = y_test
df_test["Predicted_Profit"] = y_pred_test
df_test.to_sql(TABLE_OUTPUT, con=engine, if_exists="replace", index=False)

# ---------------------- Plots ----------------------
plt.figure(figsize=(12,8))
plot_importance(best_model, importance_type='gain', max_num_features=10)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR,"feature_importance.png"))
plt.close()

print("✓ Training pipeline complete")
