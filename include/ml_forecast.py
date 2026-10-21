import duckdb
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, r2_score

import os
_dir = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_DB = os.path.join(_dir, "warehouse.duckdb")


# ── Step 1: Load clean data from warehouse ────────────────────────────────────

def load_data():
    print("\n" + "="*55)
    print("  MERIDIAN PIPELINE — REVENUE FORECAST MODEL")
    print("="*55)
    print("\n[Step 1] Loading clean data from warehouse.duckdb...")

    con = duckdb.connect(WAREHOUSE_DB)
    df = con.execute("SELECT * FROM meridian_daily").fetchdf()
    con.close()

    print(f"  ✅ {len(df)} clean rows loaded from meridian_daily")
    print(f"  Columns: {list(df.columns)}")
    return df


# ── Step 2: Feature Engineering ───────────────────────────────────────────────

def prepare_features(df: pd.DataFrame):
    print("\n[Step 2] Preparing features...")

    # Convert order_date from int (epoch ms) to datetime
    df["order_date"] = pd.to_datetime(df["order_date"], unit="ms")
    df["day_of_week"] = df["order_date"].dt.dayofweek   # 0=Mon, 6=Sun
    df["month"]       = df["order_date"].dt.month

    # Encode categorical columns → numbers (ML models need numbers)
    le = LabelEncoder()
    df["region_enc"]   = le.fit_transform(df["region"])
    df["category_enc"] = le.fit_transform(df["category"])

    print(f"  ✅ Features created: day_of_week, month, region_enc, category_enc")

    # Features (X) and Target (y)
    features = ["day_of_week", "month", "region_enc", "category_enc"]
    X = df[features]
    y = df["revenue"]

    return X, y, features


# ── Step 3: Train Model ───────────────────────────────────────────────────────

def train_model(X, y):
    print("\n[Step 3] Training Linear Regression model...")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"  Training rows: {len(X_train)} | Test rows: {len(X_test)}")

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    r2  = r2_score(y_test, y_pred)

    print(f"\n  📊 Model Performance:")
    print(f"     Mean Absolute Error : £{mae:.2f}")
    print(f"     R² Score            : {r2:.2f}  (1.0 = perfect, 0 = random)")

    return model, X_test, y_test, y_pred


# ── Step 4: Show Feature Importance ──────────────────────────────────────────

def show_feature_importance(model, features):
    print("\n[Step 4] Feature Importance (which factors drive revenue):")
    coefficients = zip(features, model.coef_)
    sorted_coef  = sorted(coefficients, key=lambda x: abs(x[1]), reverse=True)

    for feature, coef in sorted_coef:
        direction = "↑ increases" if coef > 0 else "↓ decreases"
        print(f"     {feature:<20} {direction} revenue by £{abs(coef):.2f} per unit")


# ── Step 5: Predict Next Week's Revenue ──────────────────────────────────────

def predict_next_week(model):
    print("\n[Step 5] Predicting next 7 days revenue...")

    # Simulate next 7 days: East region, Electronics category
    # region_enc: East=0, category_enc: Electronics=0
    next_week = pd.DataFrame({
        "day_of_week":  [0, 1, 2, 3, 4, 5, 6],
        "month":        [8, 8, 8, 8, 8, 8, 8],
        "region_enc":   [0, 0, 0, 0, 0, 0, 0],
        "category_enc": [0, 0, 0, 0, 0, 0, 0],
    })

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    predictions = model.predict(next_week)

    print(f"\n  {'Day':<6} {'Predicted Revenue':>18}")
    print(f"  {'-'*25}")
    for day, pred in zip(days, predictions):
        print(f"  {day:<6} £{pred:>16.2f}")

    print(f"\n  📈 Total predicted weekly revenue: £{predictions.sum():.2f}")


# ── Step 6: Data Quality Impact Demo ─────────────────────────────────────────

def show_dq_impact():
    print("\n" + "="*55)
    print("  WHY DATA QUALITY MATTERS FOR THIS MODEL")
    print("="*55)
    print("""
  The 5 rows quarantined by GX included:
    • 2 rows with null customer_id  → unknown buyer segment
    • 2 rows with duplicate order_id → double-counted revenue
    • 1 row with negative revenue    → corrupts model training

  If those 5 rows were included:
    → Model learns from wrong revenue values
    → Predictions would be skewed
    → Business makes decisions on bad forecasts

  Clean data = trustworthy model = better business decisions.
  That is the FDE's job: make the data trustworthy before it
  reaches the model.
""")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df                          = load_data()
    X, y, features              = prepare_features(df)
    model, X_test, y_test, pred = train_model(X, y)
    show_feature_importance(model, features)
    predict_next_week(model)
    show_dq_impact()

    print("="*55)
    print("  Pipeline → Warehouse → Model: connected end to end.")
    print("="*55 + "\n")
