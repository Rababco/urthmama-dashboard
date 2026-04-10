# 🌍 Urth Mama — Forecasting Pipeline & Dashboard

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the initial pipeline (generates all CSVs)
python pipeline.py "Sales_Data_Urth Mama.csv"

# 3. Launch the dashboard
streamlit run app.py
```

## Project Structure

```
urthmama_pipeline/
├── pipeline.py          # End-to-end: raw Shopify CSV → all output CSVs
├── app.py               # Streamlit dashboard (with built-in refresh)
├── requirements.txt
├── README.md
└── data/                # Auto-created by pipeline
    ├── orders_clean_eda_ready.csv
    ├── product_monthly_performance.csv
    ├── business_forecast_18m.csv
    ├── product_level_forecast_12m.csv
    ├── variant_allocated_forecast_12m.csv
    ├── abc_classification.csv
    ├── inventory_optimization.csv
    └── pipeline_metadata.json
```

## How to Refresh with New Data

### Option A: Through the Dashboard (recommended)
1. Open the dashboard: `streamlit run app.py`
2. Click **🔄 Data Refresh** in the sidebar
3. Upload your new Shopify CSV
4. Click **Run Pipeline**
5. All pages update automatically

### Option B: Command Line
```bash
python pipeline.py "path/to/new_shopify_export.csv" data
streamlit run app.py
```

## What the Pipeline Does

| Step | What happens | Output |
|------|-------------|--------|
| 1. Clean | Parses raw Shopify CSV, separates shipping/product rows, aggregates to order level | `orders_clean_eda_ready.csv`, `product_monthly_performance.csv` |
| 2. Business Forecast | ETS model selection (5 configs, AIC-based), 6-month validation, 18-month forecast | `business_forecast_18m.csv` |
| 3. Product Forecast | Per-product ETS with variant allocation | `product_level_forecast_12m.csv`, `variant_allocated_forecast_12m.csv` |
| 4. ABC Analysis | Pareto classification (80/95 thresholds) | `abc_classification.csv` |
| 5. Inventory | Safety stock, reorder points, monthly stock (adjusted by ABC class) | `inventory_optimization.csv` |

## Assumptions & Parameters

These are set as constants at the top of `pipeline.py` — adjust as needed:

- **Lead time:** 14 days
- **Service level:** 95% (Z = 1.65)
- **ABC thresholds:** A ≤ 80%, B ≤ 95%, C = rest
- **Safety stock multipliers:** A × 1.2, B × 1.0, C × 0.8
- **War months (exogenous shock):** Oct & Nov 2024 — replaced with same-month averages
- **Minimum product history:** 6 active months, 3 non-zero months

## Shopify Export Format

The pipeline expects a raw Shopify sales CSV with these columns (case-insensitive):

`Day`, `Order Name`, `Product Title`, `Product Variant Title`, `Orders`,
`Gross Sales`, `Net Sales`, `Total Sales`, `Discount Value`, `Customer Name`,
`Discount Name`, `Discount Method`, `Discount Code`, `Discount Type`,
`Shipping City`, `Shipping Country`

Export from: **Shopify Admin → Analytics → Reports → Sales**
