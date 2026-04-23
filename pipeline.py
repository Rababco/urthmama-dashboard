"""
UrthMama Forecasting Pipeline
==============================
End-to-end: Raw Shopify CSV → Clean → Forecast → ABC → Inventory → Dashboard CSVs

PIPELINE OVERVIEW:
    Step 1 — Data Cleaning: Standardize raw Shopify export, separate shipping
             from product rows, build order-level and product-monthly aggregations.
    Step 2 — Business Forecast (SARIMAX): 18-month revenue forecast using
             SARIMAX with exogenous regressors for the Oct–Nov 2024 Lebanon war
             shock and a business maturity structural break. Wholesale customers
             are excluded (sporadic B2B orders reduce accuracy; MAPE improved
             from 29% to 12% with exclusion). War months are replaced with
             same-month averages to protect seasonal estimation.
    Step 3 — Product Forecasts (Tiered ETS): 12-month unit forecasts per product.
             Tier 1 (≥24 months): full seasonal ETS. Tier 2 (13–23 months):
             seasonal-index decomposition. Tier 3 (<13 months): non-seasonal ETS.
             Sparse products (<1 unit/month) use trimmed recent mean.
             Variant-level allocation based on recent 6-month sales mix.
    Step 4 — ABC Classification: Pareto analysis ranking products by cumulative
             revenue contribution. A = top 80%, B = next 15%, C = remaining 5%.
    Step 5 — Inventory Optimization: Safety stock and reorder points at the
             variant level, using a 95% service level (z=1.65) and 14-day lead
             time. Class A products get 20% higher safety stock multiplier.

Business-level model: SARIMAX with AIC-based selection across 6 configurations
Product-level model: ETS (per-product, tiered by data availability)

Usage:
    from pipeline import run_full_pipeline
    results = run_full_pipeline("path/to/Sales_Data_Urth Mama.csv")

"""

import pandas as pd
import numpy as np
import warnings
import os
import sys
from datetime import datetime

warnings.filterwarnings('ignore')

# ─── Constants ────────────────────────────────────────────────────────────────
LEAD_TIME_DAYS = 14
SERVICE_LEVEL = 0.95
Z_SCORE = 1.65
FORECAST_HORIZON_BUSINESS = 18
FORECAST_HORIZON_PRODUCT = 12
MIN_ACTIVE_MONTHS = 6
MIN_NONZERO_MONTHS = 3
MIX_WINDOW = 6

# War months (exogenous shock — non-recurring)
WAR_MONTHS = ['2024-10-01', '2024-11-01', '2026-03-01']

# Business maturity cutoff (ramp-up period before this date)
MATURITY_DATE = '2023-07-01'

# Wholesale customers to exclude from business-level forecasting
# These are large, sporadic B2B orders that don't follow seasonal retail patterns.
# Including them inflates forecasts and reduces accuracy (MAPE 29% → 12% improvement).
WHOLESALE_CUSTOMERS = [
    'Safa Awad',
    'Wholesale Ambefrul',
    'Samira',
    'Rasha Yassine',
    'Fatima Fadel',
    'Imad Play One',
    'Wholesale Trendy Kids',
]


# ==============================================================================
# STEP 1: CLEAN RAW SHOPIFY EXPORT
# ==============================================================================

def clean_shopify_export(raw_csv_path: str) -> dict:
    """
    STEP 1: DATA CLEANING
    
    Takes a raw Shopify Sales CSV export and produces three clean datasets:
      - orders_clean: one row per order with revenue, discount, and shipping totals
      - line_items_clean: one row per product line item (shipping rows excluded)
      - product_monthly: product × variant × month aggregation for forecasting
    
    Key decisions:
      - Shipping-only rows (no product, zero quantity) are separated and aggregated
        as fees rather than treated as product sales.
      - Rows with null product_title but quantity > 0 are Shopify export gaps —
        included in order totals but excluded from product-level analysis to avoid
        creating a spurious "Unknown" product series.
      - Discount info is deduplicated at the order level to avoid double-counting
        when an order has multiple line items.
    """
    print(f"\n{'='*60}")
    print("STEP 1: CLEANING RAW SHOPIFY EXPORT")
    print(f"{'='*60}")
    print(f"  Source: {raw_csv_path}")

    df = pd.read_csv(raw_csv_path)
    print(f"  Raw rows: {len(df):,}")

    # ── Standardize column names ──
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(" ", "_", regex=False)
    )

    # ── Handle alternate Shopify export formats ──
    # New exports use different column names for the same data
    rename_map = {
        "net_items_sold": "orders",           # new format
        "discounts": "discount_value",        # new format
        "shipping_charges": "shipping_fee",   # new format (bonus)
        "order_id": "order_name",             # new format
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # ── Parse dates ──
    df["day"] = pd.to_datetime(df["day"], errors="coerce")

    # ── Parse numeric columns ──
    num_cols = [
        "product_variant_price", "discount_value",
        "orders", "gross_sales", "net_sales", "total_sales"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # ── Separate shipping rows from product rows ──
    # Shipping rows: null product title AND zero orders quantity.
    # Rows with null product_title but orders==1 are unidentified Shopify line items
    # (export gaps). We exclude them so they don't create a spurious "Unknown" series
    # in the product forecast — we keep them only for order-level revenue totals.
    df["is_shipping"] = df["product_title"].isna() & (df["orders"].fillna(0) == 0)
    df["is_product"] = ~df["is_shipping"] & df["product_title"].notna()

    # ── Build line_items_clean ──
    line_items = df[df["is_product"]].copy()

    # Ensure discount_value column exists (may be missing in some exports)
    if "discount_value" not in line_items.columns:
        line_items["discount_value"] = 0.0

    line_items = line_items[[
        "order_name", "day", "customer_name",
        "product_title", "product_variant_title",
        "orders", "gross_sales", "net_sales", "discount_value"
    ]].rename(columns={"orders": "quantity"})

    line_items["discount_value"] = line_items["discount_value"].fillna(0)
    line_items["product_variant_title"] = line_items["product_variant_title"].fillna("Standard")
    # product_title is guaranteed non-null by the is_product filter above
    line_items["customer_name"] = line_items["customer_name"].fillna("Unknown")

    # ── Build orders_clean (one row per order) ──
    shipping_by_order = (
        df[df["is_shipping"]]
        .groupby("order_name", as_index=False)
        .agg(shipping_fee=("total_sales", "sum"))
    )

    # ── Build discount aggregation (handles missing columns) ──
    discount_agg = {"discount_value_total": ("discount_value", "sum")}
    for col in ["discount_name", "discount_method", "discount_code", "discount_type"]:
        if col in df.columns:
            discount_agg[col] = (col, lambda s: s.dropna().iloc[0] if s.dropna().shape[0] else None)

    discount_by_order = (
        df[df["is_product"]]
        .groupby("order_name", as_index=False)
        .agg(**discount_agg)
    )

    orders_clean = (
        df.groupby(["order_name", "customer_name", "day"], as_index=False)
          .agg(
              items_count=("orders", "sum"),
              gross_sales=("gross_sales", "sum"),
              net_sales=("net_sales", "sum"),
              order_total_sales=("total_sales", "sum")
          )
    )

    orders_clean = orders_clean.merge(shipping_by_order, on="order_name", how="left")
    orders_clean["shipping_fee"] = orders_clean["shipping_fee"].fillna(0)

    orders_clean = orders_clean.merge(discount_by_order, on="order_name", how="left")
    orders_clean["discount_value_total"] = orders_clean["discount_value_total"].fillna(0)
    orders_clean["has_discount"] = (orders_clean["discount_value_total"] > 0).astype(int)

    # Collapse duplicates (same order_name)
    orders_collapsed = (
        orders_clean
        .groupby("order_name", as_index=False)
        .agg(
            customer_name=("customer_name", lambda x: x.dropna().iloc[0] if x.dropna().shape[0] else None),
            day=("day", "min"),
            items_count=("items_count", "sum"),
            gross_sales=("gross_sales", "sum"),
            net_sales=("net_sales", "sum"),
            order_total_sales=("order_total_sales", "sum"),
            shipping_fee=("shipping_fee", "sum"),
            discount_value_total=("discount_value_total", "sum"),
            has_discount=("has_discount", "max")
        )
    )

    # ── Build product_monthly ──
    line_items["day"] = pd.to_datetime(line_items["day"], errors="coerce")
    line_items["year"] = line_items["day"].dt.year
    line_items["month"] = line_items["day"].dt.month
    line_items["year_month"] = line_items["day"].dt.to_period("M").astype(str)

    product_monthly = (
        line_items
        .groupby(
            ["product_title", "product_variant_title", "year", "month", "year_month"],
            as_index=False
        )
        .agg(
            times_bought=("quantity", "sum"),
            total_revenue=("net_sales", "sum"),
            num_orders=("order_name", "nunique")
        )
    )

    # Mark orders that have at least one real product line item with quantity > 0
    # This matches the Promotions page filter (product_title notna AND orders > 0)
    # ensuring consistent order counts across Overview, Promotions, and Customers.
    product_order_names = set(line_items[line_items["quantity"] > 0]["order_name"].unique())
    orders_collapsed["has_product"] = orders_collapsed["order_name"].isin(product_order_names).astype(int)

    print(f"  Orders (cleaned): {len(orders_collapsed):,}")
    print(f"  Orders with products: {orders_collapsed['has_product'].sum():,}")
    print(f"  Line items: {len(line_items):,}")
    print(f"  Product-monthly records: {len(product_monthly):,}")
    print(f"  Date range: {orders_collapsed['day'].min().date()} to {orders_collapsed['day'].max().date()}")

    return {
        "orders": orders_collapsed,
        "line_items": line_items,
        "product_monthly": product_monthly
    }


# ==============================================================================
# STEP 2: BUSINESS-LEVEL FORECAST (SARIMAX)
# ==============================================================================

def build_regressors(index):
    """
    Build exogenous regressor matrix for SARIMAX.
    - war_oct24 / war_nov24: dummy variables for Lebanon war shock
    - mature: structural break indicator (0 = ramp-up, 1 = mature business)
    """
    exog = pd.DataFrame(index=index)
    exog["war_oct24"] = (index == pd.Timestamp("2024-10-01")).astype(float)
    exog["war_nov24"] = (index == pd.Timestamp("2024-11-01")).astype(float)
    exog["mature"] = (index >= pd.Timestamp(MATURITY_DATE)).astype(float)
    return exog


def run_business_forecast(orders: pd.DataFrame) -> dict:
    """
    STEP 2: BUSINESS-LEVEL REVENUE FORECAST (SARIMAX)
    
    Produces an 18-month monthly revenue forecast for retail sales.
    
    Approach:
      - Model: SARIMAX with AIC-based selection across 6 candidate configurations.
        Best model is typically SARIMAX(1,1,1)(0,1,1,12).
      - Exogenous regressors: (1) war dummies for Oct/Nov 2024 Lebanon conflict,
        (2) maturity indicator for the pre/post Jul 2023 structural break.
      - Wholesale exclusion: 7 known wholesale customers are removed before
        forecasting. Their orders are large, sporadic B2B transactions that don't
        follow seasonal patterns. Excluding them improved MAPE from ~29% to ~12%.
        A flat monthly wholesale allowance is added back to the final forecast.
      - War-month handling: mid-series war months are replaced with same-month
        averages from non-war years (protects seasonal estimation); trailing war
        months are dropped entirely.
      - Prediction intervals: built from out-of-sample forecast errors scaled
        by calendar month, not from theoretical model intervals. Floors at ±10%,
        caps at ±45%.

    NOTE: Wholesale customers are excluded from forecasting. Their orders are
    large, sporadic, and don't follow seasonal patterns — including them
    reduces forecast accuracy significantly (MAPE 29% → 12% with exclusion).
    """
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    from scipy import stats

    print(f"\n{'='*60}")
    print("STEP 2: BUSINESS-LEVEL FORECASTING (SARIMAX)")
    print(f"{'='*60}")

    orders["day"] = pd.to_datetime(orders["day"])
    
    # ── Exclude wholesale customers ──
    # Wholesale orders are large, sporadic B2B transactions that don't follow
    # seasonal retail patterns. Excluding them improves MAPE from ~29% to ~12%.
    orders_retail = orders[~orders["customer_name"].isin(WHOLESALE_CUSTOMERS)].copy()
    n_wholesale_orders = len(orders) - len(orders_retail)
    wholesale_revenue = orders["order_total_sales"].sum() - orders_retail["order_total_sales"].sum()
    
    print(f"  Excluding {n_wholesale_orders} wholesale orders (${wholesale_revenue:,.0f})")
    print(f"  Retail orders for forecasting: {len(orders_retail):,}")
    
    orders_retail["year_month"] = orders_retail["day"].dt.to_period("M").dt.to_timestamp()

    # Use product revenue only (exclude shipping fees from forecasting)
    orders_retail["product_revenue"] = orders_retail["order_total_sales"] - orders_retail["shipping_fee"]

    monthly_sales = (
        orders_retail.groupby("year_month")
        .agg(product_revenue=("product_revenue", "sum"),
             order_count=("order_name", "count"),
             items_count=("items_count", "sum"))
    )

    business_ts = monthly_sales["product_revenue"].copy()
    business_ts.index = pd.DatetimeIndex(business_ts.index)
    business_ts = business_ts.asfreq("MS").ffill()

    # ── Replace war months with same-month averages ──
    # War months (Oct/Nov 2024, Mar 2026) are non-recurring shocks.
    # Replace them with the average of the same calendar month from
    # non-war years so the seasonal pattern stays clean.
    # Trailing war months are then dropped (they add no information).
    # ── Drop trailing war/partial months first ──
    # If the last month(s) in the data are war-affected, remove them
    # before any processing. Forecasting starts from the last clean month.
    war_ym = {(pd.Timestamp(wm).year, pd.Timestamp(wm).month) for wm in WAR_MONTHS}

    while len(business_ts) > 0:
        last = business_ts.index[-1]
        if (last.year, last.month) in war_ym:
            print(f"  Dropped trailing war month: {last.strftime('%Y-%m')} "
                  f"(${business_ts.iloc[-1]:,.0f})")
            business_ts = business_ts.iloc[:-1]
        else:
            break

    # ── Replace mid-series war months with same-month averages ──
    # Oct/Nov 2024 are mid-series — can't drop without creating gaps.
    # Replace with average of same calendar month from non-war years.
    for wm in WAR_MONTHS:
        wm_ts = pd.Timestamp(wm)
        matches = business_ts[(business_ts.index.year == wm_ts.year) &
                              (business_ts.index.month == wm_ts.month)]
        if len(matches) > 0:
            same_month_clean = business_ts[
                (business_ts.index.month == wm_ts.month) &
                ~(business_ts.index.isin(matches.index))
            ]
            # Exclude other war months of same calendar month
            for owm in WAR_MONTHS:
                owm_ts = pd.Timestamp(owm)
                if owm_ts.month == wm_ts.month:
                    same_month_clean = same_month_clean[
                        ~((same_month_clean.index.year == owm_ts.year) &
                          (same_month_clean.index.month == owm_ts.month))
                    ]
            if len(same_month_clean) > 0:
                replacement = same_month_clean.mean()
                idx = matches.index[0]
                print(f"  War adjustment: {idx.strftime('%b %Y')} "
                      f"${business_ts[idx]:,.0f} -> ${replacement:,.0f}")
                business_ts[idx] = replacement

    # ── Replace Jan 2025 (delayed shipment anomaly) ──
    # A late-December shipment arrival inflated Jan 2025 revenue well above
    # normal January levels ($3.6K in 2023/2024). Replace with average of
    # other Januarys plus a growth adjustment to preserve the underlying trend.
    JAN_2025 = pd.Timestamp("2025-01-01")
    if JAN_2025 in business_ts.index:
        other_jans = business_ts[
            (business_ts.index.month == 1) &
            (business_ts.index != JAN_2025)
        ]
        if len(other_jans) > 0:
            # Apply growth factor: use ratio of recent 12-month avg to earlier 12-month avg
            recent_12 = business_ts.iloc[-12:].mean()
            earlier_12 = business_ts.iloc[-24:-12].mean() if len(business_ts) >= 24 else business_ts.iloc[:12].mean()
            growth_factor = recent_12 / earlier_12 if earlier_12 > 0 else 1.0
            replacement = other_jans.mean() * growth_factor
            original = business_ts[JAN_2025]
            print(f"  Shipment adjustment: Jan 2025 "
                  f"${original:,.0f} -> ${replacement:,.0f} "
                  f"(other Jans avg ${other_jans.mean():,.0f} x {growth_factor:.2f} growth)")
            business_ts[JAN_2025] = replacement

    print(f"  Series: {len(business_ts)} months "
          f"({business_ts.index[0].strftime('%Y-%m')} to {business_ts.index[-1].strftime('%Y-%m')})")

    # ── Train/test split ──
    TEST_SIZE = min(6, len(business_ts) // 4)
    train_ts = business_ts.iloc[:-TEST_SIZE]
    test_ts = business_ts.iloc[-TEST_SIZE:]

    train_exog = build_regressors(train_ts.index)
    test_exog = build_regressors(test_ts.index)

    print(f"  Train: {len(train_ts)} months | Test: {len(test_ts)} months")

    # ── SARIMAX model selection (AIC-based) ──
    sarimax_configs = [
        {"order": (1, 1, 1), "seasonal_order": (0, 1, 1, 12), "label": "SARIMAX(1,1,1)(0,1,1,12)"},
        {"order": (0, 1, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(0,1,1)(1,0,1,12)"},
        {"order": (1, 1, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(1,1,1)(1,0,1,12)"},
        {"order": (1, 1, 0), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(1,1,0)(1,0,1,12)"},
        {"order": (1, 0, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(1,0,1)(1,0,1,12)"},
        {"order": (2, 1, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(2,1,1)(1,0,1,12)"},
    ]

    model_results = []
    for cfg in sarimax_configs:
        try:
            model = SARIMAX(
                train_ts,
                exog=train_exog,
                order=cfg["order"],
                seasonal_order=cfg["seasonal_order"],
                enforce_stationarity=False,
                enforce_invertibility=False
            ).fit(disp=False, maxiter=500)

            model_results.append({
                "label": cfg["label"],
                "aic": model.aic,
                "model": model,
                "order": cfg["order"],
                "seasonal_order": cfg["seasonal_order"]
            })
            print(f"  {cfg['label']}: AIC = {model.aic:.2f}")
        except Exception as e:
            print(f"  {cfg['label']}: Failed - {str(e)[:60]}")

    if not model_results:
        raise RuntimeError("All SARIMAX configurations failed. Check your data.")

    model_results = sorted(model_results, key=lambda x: x["aic"])
    best = model_results[0]
    best_model = best["model"]
    best_name = best["label"]
    print(f"  >>> Best: {best_name}")

    # ── Print regressor effects ──
    print(f"\n  Regressor effects:")
    for param in ["war_oct24", "war_nov24", "mature"]:
        if param in best_model.params.index:
            coef = best_model.params[param]
            pval = best_model.pvalues[param]
            sig = "***" if pval < 0.01 else "**" if pval < 0.05 else "*" if pval < 0.1 else ""
            print(f"    {param}: ${coef:,.0f} (p={pval:.3f}) {sig}")

    # ── Validation ──
    fc_result = best_model.get_forecast(steps=len(test_ts), exog=test_exog)
    test_forecast = fc_result.predicted_mean
    test_forecast.index = test_ts.index

    errors = test_ts - test_forecast
    mae = np.mean(np.abs(errors))
    rmse = np.sqrt(np.mean(errors**2))
    mape = np.mean(np.abs(errors / test_ts)) * 100
    scale = np.mean(np.abs(np.diff(train_ts.values)))
    mase = mae / scale if scale > 0 else np.inf

    naive_fc = pd.Series([train_ts.iloc[-1]] * len(test_ts), index=test_ts.index)
    naive_mape = np.mean(np.abs((test_ts - naive_fc) / test_ts)) * 100

    print(f"\n  Validation: MAPE={mape:.2f}% | RMSE=${rmse:,.0f} | MASE={mase:.3f}")
    print(f"  vs Naive:   MAPE={naive_mape:.2f}% | Improvement={naive_mape - mape:.2f}%")

    # ── Final forecast on full data ──
    full_exog = build_regressors(business_ts.index)

    final_model = SARIMAX(
        business_ts,
        exog=full_exog,
        order=best["order"],
        seasonal_order=best["seasonal_order"],
        enforce_stationarity=False,
        enforce_invertibility=False
    ).fit(disp=False, maxiter=500)

    future_idx = pd.date_range(
        business_ts.index[-1] + pd.offsets.MonthBegin(1),
        periods=FORECAST_HORIZON_BUSINESS, freq='MS'
    )
    future_exog = build_regressors(future_idx)

    fc_result_full = final_model.get_forecast(steps=FORECAST_HORIZON_BUSINESS, exog=future_exog)
    forecast = fc_result_full.predicted_mean
    forecast.index = future_idx

    # ══════════════════════════════════════════════════════════════════════
    # ══════════════════════════════════════════════════════════════════════
    # PREDICTION INTERVALS: Based on out-of-sample forecast accuracy
    # ══════════════════════════════════════════════════════════════════════
    # Uses per-month test errors (as % of actual) where available,
    # overall MAPE for non-test months. This scales naturally: high
    # months get wider dollar bands, low months get narrow ones.

    test_ape = np.abs((test_ts - test_forecast) / test_ts)  # per-month APE

    overall_mape_frac = mape / 100

    monthly_spread = {}
    for m in range(1, 13):
        month_errors = test_ape[test_ape.index.month == m]
        if len(month_errors) > 0:
            monthly_spread[m] = float(month_errors.mean())
        else:
            monthly_spread[m] = overall_mape_frac

    # Floor 10%, cap 45%
    for m in monthly_spread:
        monthly_spread[m] = max(0.10, min(monthly_spread[m], 0.45))

    print(f"\n  Interval spread by month:")
    month_names = ['Jan','Feb','Mar','Apr','May','Jun',
                   'Jul','Aug','Sep','Oct','Nov','Dec']
    for m in range(1, 13):
        src = "test" if any(test_ape.index.month == m) else "overall"
        print(f"    {month_names[m-1]}: +/-{monthly_spread[m]:.0%} ({src})")

    lower_80, upper_80, lower_95, upper_95 = [], [], [], []

    for h in range(FORECAST_HORIZON_BUSINESS):
        fc_val = forecast.iloc[h]
        m = future_idx[h].month
        spread = monthly_spread[m]

        margin_95 = spread * fc_val
        margin_80 = margin_95 * 0.65

        lower_80.append(max(fc_val * 0.15, fc_val - margin_80))
        upper_80.append(fc_val + margin_80)
        lower_95.append(max(fc_val * 0.15, fc_val - margin_95))
        upper_95.append(fc_val + margin_95)

    # ── Calculate wholesale allowance ──
    # Wholesale is unpredictable (sporadic B2B orders), but we include a monthly
    # allowance based on historical average so total forecast reflects full business.
    total_months = len(business_ts)
    avg_wholesale_monthly = wholesale_revenue / total_months if total_months > 0 else 0
    
    print(f"\n  Wholesale allowance: ${avg_wholesale_monthly:,.0f}/month (based on ${wholesale_revenue:,.0f} over {total_months} months)")

    forecast_df = pd.DataFrame({
        'year_month': future_idx,
        'forecast_retail': forecast.values,
        'wholesale_allowance': [avg_wholesale_monthly] * len(future_idx),
        'forecast': forecast.values + avg_wholesale_monthly,  # Combined total
        'lower_80': lower_80,
        'upper_80': [u + avg_wholesale_monthly for u in upper_80],
        'lower_95': lower_95,
        'upper_95': [u + avg_wholesale_monthly for u in upper_95]
    })

    print(f"\n  Forecast: {future_idx[0].strftime('%Y-%m')} -> {future_idx[-1].strftime('%Y-%m')}")
    print(f"  Retail projected:    ${forecast.sum():,.0f}")
    print(f"  Wholesale allowance: ${avg_wholesale_monthly * len(future_idx):,.0f}")
    print(f"  Total projected:     ${forecast_df['forecast'].sum():,.0f}")
    avg_width_95 = (forecast_df['upper_95'] - forecast_df['lower_95']).mean()
    print(f"  Avg 95% interval width: ${avg_width_95:,.0f}")

    return {
        "forecast_df": forecast_df,
        "model_name": best_name,
        "mape": mape,
        "rmse": rmse,
        "mase": mase,
        "naive_mape": naive_mape,
        "monthly_sales": monthly_sales,
        "wholesale_monthly_avg": avg_wholesale_monthly
    }


# ==============================================================================
# STEP 3: PRODUCT-LEVEL FORECAST (ETS per product)
# ==============================================================================

def run_product_forecasts(product_monthly: pd.DataFrame) -> dict:
    """
    STEP 3: PRODUCT-LEVEL UNIT FORECASTS (Tiered ETS)
    
    Produces 12-month unit forecasts for each forecastable product, then
    allocates to variants based on recent sales mix.
    
    Approach:
      - Products must have ≥6 active months and ≥3 nonzero months to qualify.
      - Tiered model selection based on data availability:
          Tier 1 (≥24 months): Seasonal ETS with additive seasonality (12-period).
          Tier 2 (13–23 months): Seasonal-index decomposition — ETS requires 2
              full seasonal cycles, so instead we estimate a monthly index from
              1 cycle and apply it to a smoothed demand level.
          Tier 3 (<13 months): Non-seasonal ETS (trend only, no seasonality).
          Sparse (<1 unit/month): Trimmed recent mean — no model needed.
      - All tiers prefer damped trends to avoid explosive extrapolation.
      - Zero-filling: months with no sales are explicitly filled with zeros
        before modeling. Without this, ETS treats consecutive nonzero rows as
        consecutive months, silently corrupting trend and seasonal estimates.
      - Variant allocation: product-level forecasts are split across variants
        (colors, sizes) proportionally based on the most recent 6-month sales mix.
      - Evaluation: seasonal holdout on Jul–Sep peak window. Only products with
        ≥1 unit/month mean demand and ≥6 months of pre-holdout history are evaluated.
    """
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    print(f"\n{'='*60}")
    print("STEP 3: PRODUCT-LEVEL FORECASTING")
    print(f"{'='*60}")

    product_monthly["year_month"] = pd.to_datetime(product_monthly["year_month"])

    # ── Build prod_ts WITH zero-fill ──
    # Without zero-filling, ETS receives only months with sales and treats
    # consecutive rows as consecutive months — gaps silently corrupt
    # trend and seasonal estimates.
    all_products_list = product_monthly["product_title"].unique()
    global_min = product_monthly["year_month"].min()
    global_max = product_monthly["year_month"].max()

    raw_ts = (
        product_monthly
        .groupby(["year_month", "product_title"], as_index=False)
        .agg(units=("times_bought", "sum"), revenue=("total_revenue", "sum"))
    )

    # Zero-fill each product from its first sale month to global_max
    filled_frames = []
    for product in all_products_list:
        sub = raw_ts[raw_ts["product_title"] == product][["year_month", "units", "revenue"]].set_index("year_month")
        first_sale = sub.index.min()
        idx = pd.date_range(first_sale, global_max, freq="MS")
        sub = sub.reindex(idx, fill_value=0)
        sub["product_title"] = product
        sub.index.name = "year_month"
        filled_frames.append(sub.reset_index())

    prod_ts = pd.concat(filled_frames, ignore_index=True)

    prod_stats = (
        prod_ts.groupby("product_title", as_index=False)
        .agg(
            lifetime_units=("units", "sum"),
            active_months=("year_month", "nunique"),
            nonzero_months=("units", lambda s: (s > 0).sum())
        )
    )

    forecast_products = prod_stats[
        (prod_stats["lifetime_units"] > 0) &
        (prod_stats["active_months"] >= MIN_ACTIVE_MONTHS) &
        (prod_stats["nonzero_months"] >= MIN_NONZERO_MONTHS)
    ].sort_values("lifetime_units", ascending=False)

    print(f"  Forecastable products: {len(forecast_products)} / {prod_stats['product_title'].nunique()}")

    def forecast_product_ets(series, horizon=FORECAST_HORIZON_PRODUCT):
        """
        Tiered forecasting matched to data characteristics:

        - Sparse (mean < 1/mo): trimmed recent mean — no model needed
        - Tier 1 (≥ 24 months): seasonal ETS with 2 full cycles
        - Tier 2 (13–23 months): seasonal-index decomposition
            (ETS seasonal needs 2 full cycles; instead we estimate a
            monthly index from 1 cycle and apply it to a smoothed level)
        - Tier 3 (< 13 months): non-seasonal ETS or mean fallback
        """
        series = series.sort_index().clip(lower=0)
        series.index = pd.to_datetime(series.index)
        future_idx = pd.date_range(
            series.index[-1] + pd.offsets.MonthBegin(1), periods=horizon, freq="MS"
        )

        if len(series) < 3 or series.sum() == 0:
            return pd.Series([0.0] * horizon, index=future_idx)
        if series.nunique() <= 1:
            return pd.Series([series.mean()] * horizon, index=future_idx)

        n = len(series)
        mean_demand = series.mean()

        # ── Sparse / very low volume ──
        if mean_demand < 1.0:
            recent_mean = series.iloc[-min(12, n):].mean()
            return pd.Series([max(0, round(recent_mean, 1))] * horizon, index=future_idx)

        # ── Tier 1: ≥ 24 months — full seasonal ETS ──
        if n >= 24:
            for damped in [True, False]:
                for trend in ["add", None]:
                    try:
                        m = ExponentialSmoothing(
                            series, trend=trend,
                            damped_trend=damped if trend else False,
                            seasonal="add", seasonal_periods=12,
                            initialization_method="estimated"
                        ).fit(optimized=True)
                        fc = m.forecast(horizon)
                        fc.index = future_idx
                        return fc.clip(lower=0)
                    except Exception:
                        continue

        # ── Tier 2: 13–23 months — seasonal index + smoothed level ──
        if n >= 13:
            level = mean_demand
            for damped in [True, False]:
                for trend in ["add", None]:
                    try:
                        m = ExponentialSmoothing(
                            series, trend=trend,
                            damped_trend=damped if trend else False,
                            seasonal=None,
                            initialization_method="estimated"
                        ).fit(optimized=True)
                        level = float(m.fittedvalues.iloc[-6:].mean())
                        break
                    except Exception:
                        continue
                else:
                    continue
                break

            level = max(level, 0.1)
            monthly_avg = series.groupby(series.index.month).mean()
            overall_avg = monthly_avg.mean()
            if overall_avg > 0:
                s_index = (monthly_avg / overall_avg).reindex(range(1, 13), fill_value=1.0)
            else:
                s_index = pd.Series(1.0, index=range(1, 13))

            return pd.Series(
                [max(0, level * s_index.get(dt.month, 1.0)) for dt in future_idx],
                index=future_idx
            )

        # ── Tier 3: < 13 months — non-seasonal ETS ──
        for damped in [True, False]:
            for trend in ["add", None]:
                try:
                    m = ExponentialSmoothing(
                        series, trend=trend,
                        damped_trend=damped if trend else False,
                        seasonal=None,
                        initialization_method="estimated"
                    ).fit(optimized=True)
                    fc = m.forecast(horizon)
                    fc.index = future_idx
                    return fc.clip(lower=0)
                except Exception:
                    continue

        return pd.Series([mean_demand] * horizon, index=future_idx)

    product_forecasts = []
    for i, product in enumerate(forecast_products['product_title']):
        series = prod_ts[prod_ts['product_title'] == product].set_index('year_month')['units'].sort_index()
        series.index = pd.to_datetime(series.index)
        fc = forecast_product_ets(series)
        product_forecasts.append(pd.DataFrame({
            'year_month': fc.index,
            'product_title': product,
            'forecast_units': fc.values.round(0).astype(int)
        }))
        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(forecast_products)}...")

    product_forecasts_df = pd.concat(product_forecasts, ignore_index=True)
    print(f"  Done! {len(product_forecasts_df)} forecast records.")

    # ── Seasonal holdout evaluation ──
    # Only run on products that satisfy all three criteria:
    #   1. Active in the seasonal peak window (Jul–Aug–Sep of the last full year)
    #   2. At least 6 months of training data before that window
    #   3. Mean demand ≥ 1 unit/month (sparse products excluded — MAPE is
    #      structurally unreliable when actuals are near zero)
    #
    # Holdout = Jul–Sep of the last complete peak season in the data.
    # For data ending Dec 2025, that is Jul–Sep 2025.
    # Training = everything strictly before Jul 2025.
    # This ensures both train and test sit in the same seasonal position,
    # so the model is judged on whether it correctly forecasts the peak —
    # the most operationally important question for inventory planning.
    HOLDOUT = 3
    last_date = prod_ts["year_month"].max()
    # Find the most recent Sep in the data (end of peak window)
    last_sep = last_date - pd.DateOffset(months=(last_date.month - 9) % 12)
    if last_sep > last_date:
        last_sep -= pd.DateOffset(months=12)
    holdout_months = pd.date_range(
        last_sep - pd.DateOffset(months=HOLDOUT - 1), last_sep, freq="MS"
    )
    holdout_cutoff = holdout_months[0]  # training must end before this

    print(f"  Seasonal holdout: {holdout_months[0].strftime('%Y-%m')} → "
          f"{holdout_months[-1].strftime('%Y-%m')}")

    eval_records = []
    for product in forecast_products["product_title"]:
        series = prod_ts[
            prod_ts["product_title"] == product
        ].set_index("year_month")["units"].sort_index()
        series.index = pd.to_datetime(series.index)

        # Criterion 1: must have data in all 3 holdout months
        test_s = series[series.index.isin(holdout_months)]
        if len(test_s) < HOLDOUT:
            continue

        # Criterion 2: enough training history before the holdout
        train_s = series[series.index < holdout_cutoff]
        if len(train_s) < MIN_ACTIVE_MONTHS:
            continue

        # Criterion 3: not a sparse product
        if train_s.mean() < 1.0:
            continue

        # Skip if all test values are zero (nothing to measure against)
        if test_s.sum() == 0:
            continue

        try:
            fc = forecast_product_ets(train_s, horizon=HOLDOUT)
            fc_vals = fc.values[:HOLDOUT]
            actual = test_s.values

            mae = float(np.mean(np.abs(actual - fc_vals)))

            nonzero = actual > 0
            mape = (
                float(np.mean(np.abs(
                    (actual[nonzero] - fc_vals[nonzero]) / actual[nonzero]
                )) * 100)
                if nonzero.any() else np.nan
            )

            scale = float(np.mean(np.abs(np.diff(train_s.values)))) if len(train_s) > 1 else 1.0
            mase = float(mae / scale) if scale > 0 else np.nan

            eval_records.append({
                "product_title": product,
                "train_months": len(train_s),
                "holdout": f"{holdout_months[0].strftime('%Y-%m')} – {holdout_months[-1].strftime('%Y-%m')}",
                "actual_units": int(test_s.sum()),
                "forecast_units": int(np.round(fc_vals.sum())),
                "mae": round(mae, 1),
                "mape": round(mape, 1) if not np.isnan(mape) else None,
                "mase": round(mase, 3) if not np.isnan(mase) else None,
                "forecast_method": (
                    "Seasonal ETS"    if len(train_s) >= 24 else
                    "Seasonal Index"  if len(train_s) >= 13 else
                    "Non-seasonal ETS"
                ),
            })
        except Exception:
            continue

    eval_df = pd.DataFrame(eval_records)

    if len(eval_df) > 0:
        valid_mape = eval_df["mape"].dropna()
        valid_mase = eval_df["mase"].dropna()
        print(f"  Evaluated: {len(eval_df)} products "
              f"({len(forecast_products) - len(eval_df)} skipped — "
              f"not active in peak window or too sparse)")
        print(f"  Median MAPE : {valid_mape.median():.1f}%")
        print(f"  Median MASE : {valid_mase.median():.3f}")
        print(f"  Good  (≤30%): {(valid_mape <= 30).sum()}/{len(valid_mape)}")
        print(f"  OK  (30–60%): {((valid_mape > 30) & (valid_mape <= 60)).sum()}/{len(valid_mape)}")
        print(f"  Poor  (>60%): {(valid_mape > 60).sum()}/{len(valid_mape)}")
    else:
        print("  No products met evaluation criteria.")

    # ── Variant allocation ──
    last_obs_month = product_monthly['year_month'].max()
    mix_start = last_obs_month - pd.DateOffset(months=MIX_WINDOW - 1)
    recent = product_monthly[product_monthly['year_month'] >= mix_start]

    variant_mix = (
        recent.groupby(['product_title', 'product_variant_title'], as_index=False)
        .agg(units=('times_bought', 'sum'))
    )
    variant_mix['mix_share'] = variant_mix['units'] / variant_mix.groupby('product_title')['units'].transform('sum')

    allocated = product_forecasts_df.merge(
        variant_mix[['product_title', 'product_variant_title', 'mix_share']],
        on='product_title', how='left'
    )
    allocated['product_variant_title'] = allocated['product_variant_title'].fillna('Default')
    allocated['mix_share'] = allocated['mix_share'].fillna(1.0)
    allocated['forecast_units_variant'] = (allocated['forecast_units'] * allocated['mix_share']).round(0).astype(int)

    return {
        "product_forecasts_df": product_forecasts_df,
        "variant_allocated_df": allocated,
        "forecast_products": forecast_products,
        "eval_df": eval_df,
    }


# ==============================================================================
# STEP 4: ABC CLASSIFICATION
# ==============================================================================

def run_abc_analysis(product_monthly: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 4: ABC CLASSIFICATION (Pareto Analysis)
    
    Ranks all products by total revenue and classifies them:
      - Class A: top products contributing 80% of cumulative revenue (highest priority)
      - Class B: next 15% of cumulative revenue (moderate priority)
      - Class C: remaining 5% (lowest priority — candidates for discontinuation)
    
    This classification drives inventory policy: Class A products receive higher
    safety stock multipliers (1.2×) to minimize stockout risk.
    """
    print(f"\n{'='*60}")
    print("STEP 4: ABC CLASSIFICATION")
    print(f"{'='*60}")

    product_monthly["year_month"] = pd.to_datetime(product_monthly["year_month"])

    product_revenue = (
        product_monthly
        .groupby('product_title', as_index=False)
        .agg(
            total_revenue=('total_revenue', 'sum'),
            total_units=('times_bought', 'sum'),
            months_active=('year_month', 'nunique')
        )
        .sort_values('total_revenue', ascending=False)
        .reset_index(drop=True)
    )

    product_revenue['cumulative_revenue'] = product_revenue['total_revenue'].cumsum()
    product_revenue['revenue_pct'] = product_revenue['total_revenue'] / product_revenue['total_revenue'].sum() * 100
    product_revenue['cumulative_pct'] = product_revenue['cumulative_revenue'] / product_revenue['total_revenue'].sum() * 100
    product_revenue['product_rank'] = range(1, len(product_revenue) + 1)
    product_revenue['product_pct'] = product_revenue['product_rank'] / len(product_revenue) * 100

    product_revenue['abc_class'] = product_revenue['cumulative_pct'].apply(
        lambda x: 'A' if x <= 80 else ('B' if x <= 95 else 'C')
    )

    counts = product_revenue['abc_class'].value_counts()
    print(f"  A: {counts.get('A', 0)} | B: {counts.get('B', 0)} | C: {counts.get('C', 0)}")

    return product_revenue


# ==============================================================================
# STEP 5: INVENTORY OPTIMIZATION
# ==============================================================================

def run_inventory_optimization(product_monthly: pd.DataFrame, abc_df: pd.DataFrame) -> pd.DataFrame:
    """
    STEP 5: INVENTORY OPTIMIZATION
    
    Calculates safety stock and reorder points at the variant level.
    
    Parameters:
      - Service level: 95% (z-score = 1.65)
      - Lead time: 14 days
      - ABC adjustment: Class A gets 1.2× safety stock, Class B 1.0×, Class C 0.8×
    
    Demand variability is classified by coefficient of variation (CV):
      - Low (CV < 0.5): stable demand, lower safety stock needed
      - Medium (CV 0.5–1.0): moderate variability
      - High (CV > 1.0): erratic demand, higher safety stock buffers
    
    Note: Wholesale demand is included in inventory calculations (unlike the
    revenue forecast) because the business still needs stock on hand to fulfill
    wholesale orders regardless of their unpredictable timing.
    """
    print(f"\n{'='*60}")
    print("STEP 5: INVENTORY OPTIMIZATION")
    print(f"{'='*60}")

    product_monthly["year_month"] = pd.to_datetime(product_monthly["year_month"])

    # Group by product AND variant for variant-level inventory recommendations
    demand_stats = (
        product_monthly
        .groupby(['product_title', 'product_variant_title'], as_index=False)
        .agg(
            avg_monthly_demand=('times_bought', 'mean'),
            std_monthly_demand=('times_bought', 'std'),
            max_monthly_demand=('times_bought', 'max'),
            total_months=('year_month', 'nunique'),
            total_revenue=('total_revenue', 'sum')
        )
    )
    demand_stats['std_monthly_demand'] = demand_stats['std_monthly_demand'].fillna(0)
    demand_stats['cv'] = np.where(
        demand_stats['avg_monthly_demand'] > 0,
        demand_stats['std_monthly_demand'] / demand_stats['avg_monthly_demand'], 0
    )
    demand_stats['demand_variability'] = demand_stats['cv'].apply(
        lambda cv: 'Low' if cv < 0.5 else ('Medium' if cv < 1.0 else 'High')
    )

    # Get ABC class from product-level (variants inherit parent product's ABC class)
    inv = demand_stats.merge(
        abc_df[['product_title', 'abc_class']],
        on='product_title', how='left'
    )

    inv['avg_daily_demand'] = inv['avg_monthly_demand'] / 30
    inv['std_daily_demand'] = inv['std_monthly_demand'] / np.sqrt(30)

    inv['safety_stock'] = (Z_SCORE * inv['std_daily_demand'] * np.sqrt(LEAD_TIME_DAYS)).round(0)
    inv['reorder_point'] = (inv['avg_daily_demand'] * LEAD_TIME_DAYS + inv['safety_stock']).round(0)

    def adjust_ss(row):
        mult = {'A': 1.2, 'B': 1.0}.get(row.get('abc_class'), 0.8)
        return row['safety_stock'] * mult

    inv['adjusted_safety_stock'] = inv.apply(adjust_ss, axis=1).round(0)
    inv['adjusted_reorder_point'] = (inv['avg_daily_demand'] * LEAD_TIME_DAYS + inv['adjusted_safety_stock']).round(0)
    inv['recommended_monthly_stock'] = (inv['avg_monthly_demand'] + inv['adjusted_safety_stock']).round(0)

    # Prepare export format - include variant
    export = inv[[
        'product_title', 'product_variant_title', 'abc_class', 'total_revenue',
        'avg_monthly_demand', 'std_monthly_demand', 'demand_variability',
        'adjusted_safety_stock', 'adjusted_reorder_point', 'recommended_monthly_stock'
    ]].sort_values('total_revenue', ascending=False)

    export.columns = [
        'Product', 'Variant', 'ABC_Class', 'Total_Revenue',
        'Avg_Monthly_Demand', 'Std_Monthly_Demand', 'Demand_Variability',
        'Safety_Stock', 'Reorder_Point', 'Recommended_Monthly_Stock'
    ]

    print(f"  Product-variants optimized: {len(export)}")
    print(f"  Total safety stock: {export['Safety_Stock'].sum():,.0f} units")

    return export


# ==============================================================================
# MAIN: RUN FULL PIPELINE
# ==============================================================================

def run_full_pipeline(raw_csv_path: str, output_dir: str = "data") -> dict:
    """
    Complete end-to-end pipeline. Returns dict of all results.
    Saves CSVs to output_dir/.
    """
    os.makedirs(output_dir, exist_ok=True)

    start_time = datetime.now()
    print(f"\n{'#'*60}")
    print(f"  URTHMAMA FORECASTING PIPELINE")
    print(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    # Step 1: Clean
    cleaned = clean_shopify_export(raw_csv_path)

    # Step 2: Business forecast
    biz = run_business_forecast(cleaned["orders"])

    # Step 3: Product forecast
    prod = run_product_forecasts(cleaned["product_monthly"])

    # Step 4: ABC
    abc_df = run_abc_analysis(cleaned["product_monthly"])

    # Step 5: Inventory
    inv_df = run_inventory_optimization(cleaned["product_monthly"], abc_df)

    # ── Save all CSVs ──
    cleaned["orders"].to_csv(os.path.join(output_dir, "orders_clean_eda_ready.csv"), index=False)
    cleaned["product_monthly"].to_csv(os.path.join(output_dir, "product_monthly_performance.csv"), index=False)
    biz["forecast_df"].to_csv(os.path.join(output_dir, "business_forecast_18m.csv"), index=False)
    prod["product_forecasts_df"].to_csv(os.path.join(output_dir, "product_level_forecast_12m.csv"), index=False)
    prod["variant_allocated_df"].to_csv(os.path.join(output_dir, "variant_allocated_forecast_12m.csv"), index=False)
    if len(prod["eval_df"]) > 0:
        prod["eval_df"].to_csv(os.path.join(output_dir, "product_forecast_evaluation.csv"), index=False)
    abc_df.to_csv(os.path.join(output_dir, "abc_classification.csv"), index=False)
    inv_df.to_csv(os.path.join(output_dir, "inventory_optimization.csv"), index=False)

    # ── Save pipeline metadata ──
    elapsed = (datetime.now() - start_time).total_seconds()
    metadata = {
        "last_run": datetime.now().isoformat(),
        "source_file": os.path.basename(raw_csv_path),
        "date_range": f"{cleaned['orders']['day'].min().date()} to {cleaned['orders']['day'].max().date()}",
        "total_orders": int(cleaned["orders"]["has_product"].sum()),
        "model": biz["model_name"],
        "mape": round(biz["mape"], 2),
        "mase": round(biz["mase"], 3),
        "forecast_total": round(biz["forecast_df"]["forecast"].sum(), 0),
        "products_forecasted": len(prod["forecast_products"]),
        "product_eval_n": len(prod["eval_df"]),
        "product_eval_median_mape": round(float(prod["eval_df"]["mape"].dropna().median()), 1) if len(prod["eval_df"]) > 0 else None,
        "product_eval_median_mase": round(float(prod["eval_df"]["mase"].dropna().median()), 3) if len(prod["eval_df"]) > 0 else None,
        "elapsed_seconds": round(elapsed, 1)
    }

    import json
    with open(os.path.join(output_dir, "pipeline_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2, default=str)

    print(f"\n{'#'*60}")
    print(f"  PIPELINE COMPLETE in {elapsed:.1f}s")
    print(f"  All files saved to: {output_dir}/")
    print(f"{'#'*60}")

    return {
        "cleaned": cleaned,
        "business_forecast": biz,
        "product_forecast": prod,
        "abc": abc_df,
        "inventory": inv_df,
        "metadata": metadata
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pipeline.py <path_to_shopify_csv> [output_dir]")
        sys.exit(1)

    csv_path = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "data"
    run_full_pipeline(csv_path, out_dir)