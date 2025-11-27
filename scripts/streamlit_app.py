import streamlit as st
import pandas as pd
import joblib

# ---------------------- Load model and features ----------------------
@st.cache_resource
def load_artifacts():
    model = joblib.load("./models/xgb_model.pkl")
    features = joblib.load("./models/feature_list.pkl")
    encoders = joblib.load("./models/encoders.pkl")
    return model, features, encoders

model, features, encoders = load_artifacts()

st.set_page_config(page_title="Profit Prediction", layout="wide")
st.title("Profit Prediction App")

# ---------------------- User input ----------------------
st.header("Enter Values")

col1, col2, col3 = st.columns(3)

with col1:
    sales = st.number_input("Sales ($)", value=500.0, step=10.0)
    quantity = st.number_input("Quantity", value=5, step=1)
    discount = st.slider("Discount (%)", 0.0, 100.0, 10.0, 0.5) / 100

with col2:
    ship_mode = st.selectbox("Ship Mode", list(encoders["Ship Mode"].classes_))
    segment = st.selectbox("Segment", list(encoders["Segment"].classes_))
    category = st.selectbox("Category", list(encoders["Category"].classes_))

with col3:
    sub_category = st.selectbox("Sub-Category", list(encoders["Sub-Category"].classes_))
    region = st.selectbox("Region", list(encoders["Region"].classes_))
    order_month = st.slider("Order Month", 1, 12, 3)

# ---------------------- Encode categoricals ----------------------
ship_mode_encoded = encoders["Ship Mode"].transform([ship_mode])[0]
segment_encoded = encoders["Segment"].transform([segment])[0]
category_encoded = encoders["Category"].transform([category])[0]
sub_category_encoded = encoders["Sub-Category"].transform([sub_category])[0]
region_encoded = encoders["Region"].transform([region])[0]

# ---------------------- Final input (ONLY essential features) ----------------------
input_data = {
    "Sales": sales,
    "Quantity": quantity,
    "Discount": discount,
    "Order_Month": order_month,
    "Ship Mode_Encoded": ship_mode_encoded,
    "Segment_Encoded": segment_encoded,
    "Category_Encoded": category_encoded,
    "Sub-Category_Encoded": sub_category_encoded,
    "Region_Encoded": region_encoded
}

input_df = pd.DataFrame([input_data])

# ---------------------- Prediction ----------------------
if st.button("🔮 Predict Profit", type="primary"):
    try:
        predicted_profit = model.predict(input_df[features])[0]
        
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            st.metric(
                label="Predicted Profit",
                value=f"${predicted_profit:,.2f}",
                delta=f"{(predicted_profit / sales * 100):.1f}% margin" if sales > 0 else "N/A"
            )

        # ---------------------- Breakdown ----------------------
        st.subheader("Calculation Breakdown")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Sales", f"${sales:,.2f}")
            st.metric("Quantity", f"{quantity}")

        with col2:
            st.metric("Discount", f"{discount * 100:.1f}%")
            st.metric("Order Month", f"{order_month}")

        with col3:
            st.metric("Segment", segment)
            st.metric("Category", category)

    except Exception as e:
        st.error(f"Prediction failed: {e}")
