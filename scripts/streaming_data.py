import streamlit as st
import pandas as pd
import numpy as np
import joblib

st.set_page_config(page_title="Profit Prediction", layout="wide")
st.title("💰 Profit Prediction App")

# ---------------------- Load model & metadata ----------------------
model = joblib.load("./models/xgb_model.pkl")
features = joblib.load("./models/feature_list.pkl")
encoders = joblib.load("./models/encoders.pkl")

# ---------------------- Helpers ----------------------
def parse_input_number(x):
    if isinstance(x,(int,float)):
        return float(x)
    s = str(x).replace("$","").replace("(","").replace(")","").replace(" ","")
    if '.' in s and ',' in s and s.rfind(',') > s.rfind('.'):
        s = s.replace('.','').replace(',','.')
    elif ',' in s and s.count(',') == 1 and len(s.split(',')[1]) <= 3:
        s = s.replace(',','.')
    else:
        s = s.replace(',','')
    try:
        return float(s)
    except:
        raise ValueError(f"Cannot parse {x}")

def safe_encode(col,value):
    le = encoders[col]
    if str(value) not in list(le.classes_):
        return int(0) # fallback
    return int(le.transform([value])[0])

# ---------------------- Inputs ----------------------
st.header("Enter Values")
col1,col2,col3 = st.columns(3)

with col1:
    sales_raw = st.text_input("Sales ($)", "400.00")
    quantity = st.number_input("Quantity", 1, step=1)
    discount_raw = st.text_input("Discount (0.2=20%)","0.0")

with col2:
    ship_mode = st.selectbox("Ship Mode",list(encoders["Ship Mode"].classes_))
    segment   = st.selectbox("Segment",list(encoders["Segment"].classes_))
    category  = st.selectbox("Category",list(encoders["Category"].classes_))

with col3:
    sub_category = st.selectbox("Sub-Category",list(encoders["Sub-Category"].classes_))
    region       = st.selectbox("Region",list(encoders["Region"].classes_))
    order_month  = st.slider("Order Month",1,12,1)

# ---------------------- Parse inputs ----------------------
sales = parse_input_number(sales_raw)
d = float(discount_raw)
if d>1: d=d/100
discount=d

# Encode categoricals
ship_mode_enc = safe_encode("Ship Mode",ship_mode)
segment_enc   = safe_encode("Segment",segment)
category_enc  = safe_encode("Category",category)
sub_category_enc = safe_encode("Sub-Category",sub_category)
region_enc    = safe_encode("Region",region)

# ---------------------- Prepare DataFrame ----------------------
input_df = pd.DataFrame([{
    "Sales": sales,
    "Quantity": quantity,
    "Discount": discount,
    "Order_Month": order_month,
    "Ship Mode_Encoded": ship_mode_enc,
    "Segment_Encoded": segment_enc,
    "Category_Encoded": category_enc,
    "Sub-Category_Encoded": sub_category_enc,
    "Region_Encoded": region_enc
}])[features]

# ---------------------- Prediction ----------------------
if st.button("🔮 Predict Profit"):
    pred = model.predict(input_df)[0]
    margin_pct = pred/sales*100 if sales!=0 else np.nan

    st.markdown("---")
    st.metric("Predicted Profit", f"${pred:,.2f}", delta=f"{margin_pct:.1f}% margin")
