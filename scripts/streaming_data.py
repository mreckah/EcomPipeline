import pandas as pd
import random
import time
import uuid
from datetime import datetime, timedelta

class DataStreamer:
    def __init__(self, file_path, delay=5):
        self.file_path = file_path
        self.delay = delay

        # Load base dataset
        self.base_df = pd.read_csv(file_path)

        # Probabilities and mappings
        self.ship_modes = self.base_df['Ship Mode'].unique().tolist()
        self.segments = self.base_df['Segment'].unique().tolist()
        self.countries = self.base_df['Country/Region'].unique().tolist()
        self.categories = self.base_df['Category'].unique().tolist()
        self.subcategories = self.base_df['Sub-Category'].unique().tolist()

        # Compute frequencies (fallback if column missing)
        if 'Product ID' in self.base_df.columns:
            self.product_freq = self.base_df['Product ID'].value_counts(normalize=True)
        else:
            self.product_freq = pd.Series(dtype=float)

        if 'Customer ID' in self.base_df.columns:
            self.customer_freq = self.base_df['Customer ID'].value_counts(normalize=True)
        else:
            self.customer_freq = pd.Series(dtype=float)

        # Probabilities
        self.new_customer_prob = 0.05
        self.new_product_prob = 0.05

    def _random_customer(self):
        """Pick existing or new customer."""
        if len(self.customer_freq) > 0 and random.random() > self.new_customer_prob:
            return random.choices(self.customer_freq.index, weights=self.customer_freq.values, k=1)[0]
        return f"CUST-{uuid.uuid4().hex[:6]}"

    def _random_product(self):
        """Pick existing or new product."""
        if len(self.product_freq) > 0 and random.random() > self.new_product_prob:
            return random.choices(self.product_freq.index, weights=self.product_freq.values, k=1)[0]
        return f"PROD-{uuid.uuid4().hex[:6]}"

    def generate_row(self):
        """Generate a new synthetic row similar to existing dataset."""
        order_id = f"ORD-{uuid.uuid4().hex[:6]}"
        order_date = datetime.now().strftime("%Y-%m-%d")
        ship_date = (datetime.now() + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d")

        row = {
            "Row ID": len(self.base_df) + 1,
            "Order ID": order_id,
            "Order Date": order_date,
            "Ship Date": ship_date,
            "Ship Mode": random.choice(self.ship_modes),
            "Customer ID": self._random_customer(),
            "Customer Name": f"Name-{random.randint(1, 1000)}",
            "Segment": random.choice(self.segments),
            "Country/Region": random.choice(self.countries),
            "City": f"City-{random.randint(1, 500)}",
            "State": f"State-{random.randint(1, 100)}",
            "Postal Code": random.randint(10000, 99999),
            "Region": random.choice(["East", "West", "Central", "South"]),
            "Product ID": self._random_product(),
            "Category": random.choice(self.categories),
            "Sub-Category": random.choice(self.subcategories),
            "Product Name": f"Product-{random.randint(1, 500)}",
            "Sales": round(random.uniform(10, 500), 2),
            "Quantity": random.randint(1, 10),
            "Discount": round(random.uniform(0, 0.3), 2),
            "Profit": round(random.uniform(-50, 200), 2)
        }
        return row

    def stream(self):
        """Continuously append rows to CSV file."""
        while True:
            row = self.generate_row()
            new_df = pd.DataFrame([row])
            new_df.to_csv(self.file_path, mode='a', header=False, index=False)
            print(f"Added row: {row}")
            time.sleep(self.delay)

if __name__ == "__main__":
    file_path = "./data/input/sales.csv"  # adjust path if needed
    streamer = DataStreamer(file_path, delay=3)  # add new row every 3 sec
    streamer.stream()
