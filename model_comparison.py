"""
Model Comparison: ETS vs SARIMAX vs ETS+Regression Hybrid
==========================================================
Run this in VS Code to compare all three approaches on your data.

Usage:
    python model_comparison.py

Requires: pandas, numpy, statsmodels, scipy, matplotlib
Place this script in the same folder as Sales_Data_Urth Mama.csv
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.statespace.sarimax import SARIMAX
from scipy import stats

warnings.filterwarnings('ignore')

# ==============================================================================
# STEP 1: LOAD & CLEAN DATA (same as your pipeline)
# ==============================================================================
print("=" * 70)
print("LOADING AND PREPARING DATA")
print("=" * 70)

df = pd.read_csv("Sales_Data_Urth Mama.csv")
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
df["day"] = pd.to_datetime(df["day"], errors="coerce")

num_cols = ["product_variant_price", "discount_value", "orders", "gross_sales", "net_sales", "total_sales"]
for c in num_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# Aggregate to order level, then to monthly
df["year_month"] = df["day"].dt.to_period("M").dt.to_timestamp()
monthly_sales = df.groupby("year_month")["total_sales"].sum()
monthly_sales.index = pd.DatetimeIndex(monthly_sales.index)
monthly_sales = monthly_sales.asfreq("MS").ffill()

print(f"Monthly series: {len(monthly_sales)} months")
print(f"Range: {monthly_sales.index[0].strftime('%Y-%m')} to {monthly_sales.index[-1].strftime('%Y-%m')}")
print(f"Avg monthly revenue: ${monthly_sales.mean():,.0f}")

# ==============================================================================
# STEP 2: BUILD REGRESSORS
# ==============================================================================

def build_regressors(index):
    """Build exogenous regressor matrix for any date index."""
    exog = pd.DataFrame(index=index)

    # War dummies: Oct 2024, Nov 2024
    exog["war_oct24"] = (index == pd.Timestamp("2024-10-01")).astype(float)
    exog["war_nov24"] = (index == pd.Timestamp("2024-11-01")).astype(float)

    # Ramp-up indicator: 0 before July 2023, 1 after
    # (business was still growing in the first ~11 months)
    exog["mature"] = (index >= pd.Timestamp("2023-07-01")).astype(float)

    return exog


# ==============================================================================
# STEP 3: TRAIN/TEST SPLIT
# ==============================================================================

TEST_SIZE = 6
train_ts = monthly_sales.iloc[:-TEST_SIZE]
test_ts = monthly_sales.iloc[-TEST_SIZE:]

train_exog = build_regressors(train_ts.index)
test_exog = build_regressors(test_ts.index)

print(f"\nTrain: {len(train_ts)} months ({train_ts.index[0].strftime('%Y-%m')} to {train_ts.index[-1].strftime('%Y-%m')})")
print(f"Test:  {len(test_ts)} months ({test_ts.index[0].strftime('%Y-%m')} to {test_ts.index[-1].strftime('%Y-%m')})")

# Naive benchmark
naive_fc = pd.Series([train_ts.iloc[-1]] * TEST_SIZE, index=test_ts.index)
naive_mape = np.mean(np.abs((test_ts - naive_fc) / test_ts)) * 100

print(f"\nNaive MAPE (benchmark): {naive_mape:.2f}%")


# ==============================================================================
# HELPER: Evaluation metrics
# ==============================================================================

def evaluate(actual, predicted, model_name):
    """Calculate and print validation metrics."""
    errors = actual - predicted
    mae = np.mean(np.abs(errors))
    rmse = np.sqrt(np.mean(errors**2))
    mape = np.mean(np.abs(errors / actual)) * 100
    scale = np.mean(np.abs(np.diff(train_ts.values)))
    mase = mae / scale if scale > 0 else np.inf

    print(f"\n  {model_name}")
    print(f"  {'─'*45}")
    print(f"  MAE:  ${mae:,.0f}")
    print(f"  RMSE: ${rmse:,.0f}")
    print(f"  MAPE: {mape:.2f}%")
    print(f"  MASE: {mase:.3f} {'✓ beats naive' if mase < 1 else '✗ worse than naive'}")
    print(f"  Improvement vs naive: {naive_mape - mape:+.2f}%")

    return {"name": model_name, "mae": mae, "rmse": rmse, "mape": mape, "mase": mase}


# ==============================================================================
# MODEL A: CURRENT ETS (your existing approach, with war adjustment)
# ==============================================================================

print("\n" + "=" * 70)
print("MODEL A: CURRENT ETS (with war-month averaging)")
print("=" * 70)

# War adjustment (same as your current code)
train_adjusted = train_ts.copy()
for wm in ["2024-10-01", "2024-11-01"]:
    wm_ts = pd.Timestamp(wm)
    if wm_ts in train_adjusted.index:
        same_month_vals = train_ts[
            (train_ts.index.month == wm_ts.month) & (train_ts.index != wm_ts)
        ]
        if len(same_month_vals) > 0:
            train_adjusted[wm_ts] = same_month_vals.mean()

# Fit best ETS by AIC
use_seasonal = len(train_adjusted) >= 24
configs_ets = [
    {'error': 'add', 'trend': 'add', 'damped': False, 'seasonal': 'add' if use_seasonal else None},
    {'error': 'add', 'trend': 'add', 'damped': True,  'seasonal': 'add' if use_seasonal else None},
    {'error': 'mul', 'trend': 'add', 'damped': False, 'seasonal': 'add' if use_seasonal else None},
    {'error': 'mul', 'trend': 'add', 'damped': True,  'seasonal': 'add' if use_seasonal else None},
    {'error': 'mul', 'trend': 'mul', 'damped': True,  'seasonal': 'mul' if use_seasonal else None},
]

best_ets = None
best_aic = np.inf
for cfg in configs_ets:
    try:
        m = ExponentialSmoothing(
            train_adjusted,
            trend=cfg['trend'],
            damped_trend=cfg['damped'] if cfg['trend'] else False,
            seasonal=cfg['seasonal'],
            seasonal_periods=12 if cfg['seasonal'] else None,
            initialization_method='estimated', use_boxcox=False
        ).fit(optimized=True, use_brute=True)
        label = f"ETS({cfg['error'][0].upper()},{cfg['trend'][0].upper()}{'d' if cfg['damped'] else ''},{(cfg['seasonal'] or 'N')[0].upper()})"
        print(f"  {label}: AIC = {m.aic:.2f}")
        if m.aic < best_aic:
            best_aic = m.aic
            best_ets = m
            best_ets_label = label
    except:
        pass

print(f"  >>> Best: {best_ets_label}")

fc_a = best_ets.forecast(TEST_SIZE)
fc_a.index = test_ts.index
metrics_a = evaluate(test_ts, fc_a, f"Model A: {best_ets_label} (current)")


# ==============================================================================
# MODEL B: SARIMAX (with war dummies + maturity regressor)
# ==============================================================================

print("\n" + "=" * 70)
print("MODEL B: SARIMAX (with regressors)")
print("=" * 70)

# Try several SARIMAX orders (keep them low for 41 months of data)
sarimax_configs = [
    {"order": (1, 1, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(1,1,1)(1,0,1,12)"},
    {"order": (1, 1, 0), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(1,1,0)(1,0,1,12)"},
    {"order": (0, 1, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(0,1,1)(1,0,1,12)"},
    {"order": (1, 1, 1), "seasonal_order": (0, 1, 1, 12), "label": "SARIMAX(1,1,1)(0,1,1,12)"},
    {"order": (1, 0, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(1,0,1)(1,0,1,12)"},
    {"order": (2, 1, 1), "seasonal_order": (1, 0, 1, 12), "label": "SARIMAX(2,1,1)(1,0,1,12)"},
]

best_sarimax = None
best_sarimax_aic = np.inf

for cfg in sarimax_configs:
    try:
        m = SARIMAX(
            train_ts,  # NOTE: raw data, not war-adjusted — dummies handle the war
            exog=train_exog,
            order=cfg["order"],
            seasonal_order=cfg["seasonal_order"],
            enforce_stationarity=False,
            enforce_invertibility=False
        ).fit(disp=False, maxiter=500)

        print(f"  {cfg['label']}: AIC = {m.aic:.2f}")
        if m.aic < best_sarimax_aic:
            best_sarimax_aic = m.aic
            best_sarimax = m
            best_sarimax_label = cfg['label']
    except Exception as e:
        print(f"  {cfg['label']}: Failed — {str(e)[:60]}")

if best_sarimax is not None:
    print(f"  >>> Best: {best_sarimax_label}")

    fc_b_result = best_sarimax.get_forecast(steps=TEST_SIZE, exog=test_exog)
    fc_b = fc_b_result.predicted_mean
    fc_b.index = test_ts.index
    metrics_b = evaluate(test_ts, fc_b, f"Model B: {best_sarimax_label}")

    # Print regressor coefficients
    print(f"\n  Regressor effects:")
    for param_name in ["war_oct24", "war_nov24", "mature"]:
        if param_name in best_sarimax.params.index:
            coef = best_sarimax.params[param_name]
            pval = best_sarimax.pvalues[param_name]
            sig = "***" if pval < 0.01 else "**" if pval < 0.05 else "*" if pval < 0.1 else ""
            print(f"    {param_name}: ${coef:,.0f} (p={pval:.3f}) {sig}")
else:
    print("  All SARIMAX models failed!")
    metrics_b = {"name": "Model B: SARIMAX", "mape": 999, "mase": 999}


# ==============================================================================
# MODEL C: ETS + REGRESSION HYBRID
# ==============================================================================

print("\n" + "=" * 70)
print("MODEL C: ETS + REGRESSION HYBRID")
print("=" * 70)

# Step C1: Regress out known effects from the raw series
import statsmodels.api as sm

exog_with_const = sm.add_constant(train_exog)
reg_model = sm.OLS(train_ts, exog_with_const).fit()

print("  Regression step (removing known effects):")
for param_name in ["war_oct24", "war_nov24", "mature"]:
    coef = reg_model.params[param_name]
    pval = reg_model.pvalues[param_name]
    sig = "***" if pval < 0.01 else "**" if pval < 0.05 else "*" if pval < 0.1 else ""
    print(f"    {param_name}: ${coef:,.0f} (p={pval:.3f}) {sig}")

# Remove regressor effects (keep the intercept — we're cleaning, not detrending)
regressor_effect_train = reg_model.predict(exog_with_const) - reg_model.params["const"]
cleaned_train = train_ts - regressor_effect_train + train_ts.mean()  # re-center

# Step C2: Fit ETS on the cleaned series
best_hybrid_ets = None
best_hybrid_aic = np.inf

for cfg in configs_ets:
    try:
        m = ExponentialSmoothing(
            cleaned_train,
            trend=cfg['trend'],
            damped_trend=cfg['damped'] if cfg['trend'] else False,
            seasonal=cfg['seasonal'],
            seasonal_periods=12 if cfg['seasonal'] else None,
            initialization_method='estimated', use_boxcox=False
        ).fit(optimized=True, use_brute=True)
        label = f"ETS({cfg['error'][0].upper()},{cfg['trend'][0].upper()}{'d' if cfg['damped'] else ''},{(cfg['seasonal'] or 'N')[0].upper()})"
        print(f"  Hybrid {label}: AIC = {m.aic:.2f}")
        if m.aic < best_hybrid_aic:
            best_hybrid_aic = m.aic
            best_hybrid_ets = m
            best_hybrid_label = label
    except:
        pass

print(f"  >>> Best hybrid ETS: {best_hybrid_label}")

# Step C3: Forecast with hybrid = ETS forecast + regressor effects for future
fc_c_ets = best_hybrid_ets.forecast(TEST_SIZE)
fc_c_ets.index = test_ts.index

test_exog_with_const = test_exog.copy()
test_exog_with_const.insert(0, "const", 1.0)  # manually add constant to match training shape
regressor_effect_test = reg_model.predict(test_exog_with_const) - reg_model.params["const"]

fc_c = fc_c_ets + regressor_effect_test - train_ts.mean() + reg_model.params["const"]
fc_c = fc_c.clip(lower=0)

metrics_c = evaluate(test_ts, fc_c, f"Model C: Hybrid (Regression + {best_hybrid_label})")


# ==============================================================================
# STEP 4: PREDICTION INTERVAL COMPARISON
# ==============================================================================

print("\n" + "=" * 70)
print("PREDICTION INTERVAL COMPARISON")
print("=" * 70)

FORECAST_HORIZON = 18

# --- Model A intervals: current empirical method ---
# Refit on full data (war-adjusted)
full_adjusted = monthly_sales.copy()
for wm in ["2024-10-01", "2024-11-01"]:
    wm_ts = pd.Timestamp(wm)
    if wm_ts in full_adjusted.index:
        same_month = monthly_sales[(monthly_sales.index.month == wm_ts.month) & (monthly_sales.index != wm_ts)]
        if len(same_month) > 0:
            full_adjusted[wm_ts] = same_month.mean()

ets_full = ExponentialSmoothing(
    full_adjusted, trend=best_ets.params_formatted.get('smoothing_trend', None) and 'add',
    seasonal='add' if use_seasonal else None,
    seasonal_periods=12 if use_seasonal else None,
    initialization_method='estimated', use_boxcox=False
).fit(optimized=True, use_brute=True)

fc_full_a = ets_full.forecast(FORECAST_HORIZON)
future_idx = pd.date_range(monthly_sales.index[-1] + pd.offsets.MonthBegin(1), periods=FORECAST_HORIZON, freq='MS')
fc_full_a.index = future_idx

# Empirical intervals (current method)
ref_ts = full_adjusted.drop(pd.Timestamp("2022-08-01"), errors='ignore')
empirical_widths = []
for dt, fc_val in zip(future_idx, fc_full_a.values):
    m = dt.month
    vals = ref_ts[ref_ts.index.month == m].values
    if len(vals) >= 2:
        mn = np.mean(vals)
        low95 = max(0, fc_val * np.percentile(vals, 2.5) / mn)
        high95 = fc_val * np.percentile(vals, 97.5) / mn
    else:
        low95 = fc_val * 0.5
        high95 = fc_val * 1.5
    empirical_widths.append(high95 - low95)

avg_empirical_width = np.mean(empirical_widths)
print(f"\n  Model A (empirical intervals):")
print(f"    Avg 95% interval width: ${avg_empirical_width:,.0f}")

# --- Model B intervals: SARIMAX parametric ---
if best_sarimax is not None:
    full_exog = build_regressors(monthly_sales.index)
    try:
        sarimax_full = SARIMAX(
            monthly_sales, exog=full_exog,
            order=best_sarimax.specification['order'],
            seasonal_order=best_sarimax.specification['seasonal_order'],
            enforce_stationarity=False, enforce_invertibility=False
        ).fit(disp=False, maxiter=500)

        future_exog = build_regressors(future_idx)
        fc_full_b_result = sarimax_full.get_forecast(steps=FORECAST_HORIZON, exog=future_exog)
        fc_full_b = fc_full_b_result.predicted_mean
        ci_b = fc_full_b_result.conf_int(alpha=0.05)

        sarimax_widths = (ci_b.iloc[:, 1] - ci_b.iloc[:, 0]).values
        avg_sarimax_width = np.mean(sarimax_widths)
        print(f"\n  Model B (SARIMAX parametric intervals):")
        print(f"    Avg 95% interval width: ${avg_sarimax_width:,.0f}")
    except Exception as e:
        print(f"\n  Model B interval calculation failed: {e}")
        avg_sarimax_width = None

# --- Model C intervals: bootstrapped from hybrid residuals ---
hybrid_resid = best_hybrid_ets.resid.dropna()
resid_std = hybrid_resid.std()

# Re-fit hybrid on full data
full_exog_wc = sm.add_constant(build_regressors(monthly_sales.index))
reg_full = sm.OLS(monthly_sales, full_exog_wc).fit()
reg_effect_full = reg_full.predict(full_exog_wc) - reg_full.params["const"]
cleaned_full = monthly_sales - reg_effect_full + monthly_sales.mean()

ets_hybrid_full = ExponentialSmoothing(
    cleaned_full, trend='add', damped_trend=True,
    seasonal='add' if use_seasonal else None,
    seasonal_periods=12 if use_seasonal else None,
    initialization_method='estimated', use_boxcox=False
).fit(optimized=True, use_brute=True)

fc_ets_hybrid_full = ets_hybrid_full.forecast(FORECAST_HORIZON)
fc_ets_hybrid_full.index = future_idx

future_exog_wc = build_regressors(future_idx).copy()
future_exog_wc.insert(0, "const", 1.0)
reg_effect_future = reg_full.predict(future_exog_wc) - reg_full.params["const"]
fc_full_c = fc_ets_hybrid_full + reg_effect_future - monthly_sales.mean() + reg_full.params["const"]

# Bootstrap intervals: use residual std with increasing uncertainty
bootstrap_widths = []
for h in range(1, FORECAST_HORIZON + 1):
    width = 2 * 1.96 * resid_std * np.sqrt(h)  # grows with horizon
    bootstrap_widths.append(width)
avg_bootstrap_width = np.mean(bootstrap_widths)
print(f"\n  Model C (bootstrap intervals from hybrid residuals):")
print(f"    Avg 95% interval width: ${avg_bootstrap_width:,.0f}")

# Interval improvement
print(f"\n  Interval width comparison (lower = tighter):")
print(f"    Model A (empirical):  ${avg_empirical_width:,.0f}  ← current")
if avg_sarimax_width:
    pct_b = (avg_empirical_width - avg_sarimax_width) / avg_empirical_width * 100
    print(f"    Model B (SARIMAX):    ${avg_sarimax_width:,.0f}  ({pct_b:+.1f}%)")
pct_c = (avg_empirical_width - avg_bootstrap_width) / avg_empirical_width * 100
print(f"    Model C (hybrid):     ${avg_bootstrap_width:,.0f}  ({pct_c:+.1f}%)")


# ==============================================================================
# STEP 5: SUMMARY TABLE
# ==============================================================================

print("\n" + "=" * 70)
print("FINAL COMPARISON")
print("=" * 70)

results = [metrics_a, metrics_b, metrics_c]
results_sorted = sorted(results, key=lambda x: x["mape"])

print(f"\n  {'Model':<50} {'MAPE':>8} {'MASE':>8} {'RMSE':>12}")
print(f"  {'─'*80}")
for r in results_sorted:
    winner = " ★ BEST" if r == results_sorted[0] else ""
    print(f"  {r['name']:<50} {r['mape']:>7.2f}% {r['mase']:>8.3f} ${r['rmse']:>10,.0f}{winner}")

print(f"\n  Naive benchmark MAPE: {naive_mape:.2f}%")

best = results_sorted[0]
print(f"\n  ═══════════════════════════════════════════")
print(f"  RECOMMENDATION: {best['name']}")
print(f"  MAPE: {best['mape']:.2f}% | MASE: {best['mase']:.3f}")
print(f"  ═══════════════════════════════════════════")


# ==============================================================================
# STEP 6: VISUALIZATION
# ==============================================================================

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle("Model Comparison: ETS vs SARIMAX vs Hybrid", fontsize=14, fontweight='bold')

# Plot 1: Test set comparison
ax1 = axes[0, 0]
ax1.plot(test_ts.index, test_ts.values, 'ko-', lw=2, ms=8, label='Actual', zorder=5)
ax1.plot(test_ts.index, fc_a.values, 'b^--', ms=7, label=f'A: {best_ets_label}')
if best_sarimax is not None:
    ax1.plot(test_ts.index, fc_b.values, 'rs--', ms=7, label=f'B: SARIMAX')
ax1.plot(test_ts.index, fc_c.values, 'gD--', ms=7, label='C: Hybrid')
ax1.plot(test_ts.index, naive_fc.values, ':', color='gray', alpha=0.5, label='Naive')
ax1.set_title('Test Set Forecast Comparison')
ax1.set_ylabel('Revenue ($)')
ax1.legend(fontsize=8)
ax1.grid(True, alpha=0.3)

# Plot 2: Full series + 18-month forecasts with intervals
ax2 = axes[0, 1]
ax2.plot(monthly_sales.index, monthly_sales.values, 'k-', lw=1.5, label='Historical')

if best_sarimax is not None and avg_sarimax_width is not None:
    ax2.plot(future_idx, fc_full_b.values, 'r--', lw=2, label='B: SARIMAX')
    ax2.fill_between(future_idx, ci_b.iloc[:, 0], ci_b.iloc[:, 1], color='red', alpha=0.15, label='B: 95% PI')

# Hybrid forecast + intervals
hybrid_low = fc_full_c.values - 1.96 * resid_std * np.sqrt(np.arange(1, FORECAST_HORIZON + 1))
hybrid_high = fc_full_c.values + 1.96 * resid_std * np.sqrt(np.arange(1, FORECAST_HORIZON + 1))
ax2.plot(future_idx, fc_full_c.values, 'g--', lw=2, label='C: Hybrid')
ax2.fill_between(future_idx, np.maximum(hybrid_low, 0), hybrid_high, color='green', alpha=0.15, label='C: 95% PI')

ax2.axvline(x=monthly_sales.index[-1], color='gray', ls=':', alpha=0.7)
ax2.set_title('18-Month Forecasts with Prediction Intervals')
ax2.set_ylabel('Revenue ($)')
ax2.legend(fontsize=7, loc='upper left')
ax2.grid(True, alpha=0.3)

# Plot 3: Interval widths by month
ax3 = axes[1, 0]
months_ahead = range(1, FORECAST_HORIZON + 1)
ax3.plot(months_ahead, empirical_widths, 'b-o', ms=5, label='A: Empirical (current)')
if avg_sarimax_width is not None:
    ax3.plot(months_ahead, sarimax_widths, 'r-s', ms=5, label='B: SARIMAX parametric')
ax3.plot(months_ahead, bootstrap_widths, 'g-D', ms=5, label='C: Hybrid bootstrap')
ax3.set_title('95% Prediction Interval Width Over Forecast Horizon')
ax3.set_xlabel('Months Ahead')
ax3.set_ylabel('Interval Width ($)')
ax3.legend(fontsize=8)
ax3.grid(True, alpha=0.3)

# Plot 4: Residual diagnostics of best model
ax4 = axes[1, 1]
if best_sarimax is not None:
    resids_best = best_sarimax.resid.dropna()
    ax4.set_title(f'Residuals: Best SARIMAX')
else:
    resids_best = hybrid_resid
    ax4.set_title(f'Residuals: Hybrid ETS')
ax4.hist(resids_best, bins=15, color='steelblue', edgecolor='black', alpha=0.7, density=True)
x_range = np.linspace(resids_best.min(), resids_best.max(), 100)
ax4.plot(x_range, stats.norm.pdf(x_range, resids_best.mean(), resids_best.std()), 'r-', lw=2)
ax4.set_xlabel('Residual')
ax4.set_ylabel('Density')
ax4.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("model_comparison.png", dpi=150, bbox_inches='tight')
plt.show()

print("\n✅ Chart saved: model_comparison.png")
print("\nCopy the MAPE/MASE results and share them with Claude to decide which model to integrate into the pipeline.")
