"""
Urth Mama Analytics Dashboard
MSBA Capstone Project - Rabab Ali Swaidan

With integrated data refresh pipeline.

To run:
    pip install streamlit pandas numpy plotly statsmodels scipy
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import os
import json

# ==============================================================================
# PAGE CONFIG
# ==============================================================================
st.set_page_config(
    page_title="Urth Mama Analytics",
    page_icon=":earth_americas:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# AUTHENTICATION
# ==============================================================================
USERS = {
    "bs@urthmama": {"password": "urthmama1", "name": "Batoul Swayden"},
    "ha@urthmama": {"password": "urthmama2", "name": "Hind El Ammouri"},
    "rs@aub":      {"password": "aub",       "name": "Rabab Swaidan"},
}

def check_password():
    """Gate the entire app behind a login form. Returns True if authenticated."""
    if st.session_state.get("authenticated"):
        return True

    # ── Hide sidebar and header on login page ──
    st.markdown("""
    <style>
        [data-testid="stSidebar"] { display: none !important; }
        [data-testid="collapsedControl"] { display: none !important; }
        [data-testid="stHeader"] { display: none !important; }
        header { display: none !important; }
        .block-container {
            max-width: 400px !important;
            padding-top: 20vh !important;
            margin: 0 auto !important;
        }
    </style>
    """, unsafe_allow_html=True)

    # Logo
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        st.image("urth_mama_logo.png", width=150)

    st.markdown(
        "<p style='text-align:center; font-family:'Courier New',Courier,monospace; "
        "color:#5a9a8f; margin-bottom:24px;'>Enter your credentials to continue</p>",
        unsafe_allow_html=True
    )

    username = st.text_input("Username", placeholder="e.g. rs@aub")
    password = st.text_input("Password", type="password", placeholder="Enter password")

    if st.button("Log in", type="primary", use_container_width=True):
        user = USERS.get(username)
        if user and user["password"] == password:
            st.session_state["authenticated"] = True
            st.session_state["user_name"] = user["name"]
            st.session_state["user_id"] = username
            st.rerun()
        else:
            st.error("Invalid username or password.")

    return False


if not check_password():
    st.stop()

# ==============================================================================
# CUSTOM CSS
# ==============================================================================
st.markdown("""
<style>
    .stApp { background: linear-gradient(135deg, #f0f7f4 0%, #e8f4f8 100%); }
    h1, h2, h3 { font-family: 'Courier New', Courier, monospace !important; color: #2a9d8f !important; font-weight: 700 !important; }
    p, span, label, .stMarkdown { font-family: 'Courier New', Courier, monospace !important; }
    body, .stApp, [class*="css"] { font-family: 'Courier New', Courier, monospace !important; }
    [data-testid="metric-container"] {
        background: linear-gradient(145deg, #ffffff, #f0f7f4);
        border: 1px solid #b8e0d9; border-radius: 16px; padding: 20px;
        box-shadow: 0 4px 15px rgba(42, 157, 143, 0.1);
    }
    [data-testid="metric-container"] label { color: #5a9a8f !important; font-weight: 600 !important; text-transform: uppercase; font-size: 0.75rem !important; }
    [data-testid="metric-container"] [data-testid="stMetricValue"] { color: #1a6b5f !important; font-family: 'Courier New', Courier, monospace !important; font-size: 1.8rem !important; font-weight: 700 !important; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #1e6091 0%, #168aad 50%, #2a9d8f 100%); }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #ffffff !important; }
    [data-testid="stSidebar"] .stSelectbox label, [data-testid="stSidebar"] .stRadio label { color: #d4f1f4 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { background: #d4f1f4; border-radius: 8px; color: #1a6b5f; font-family: 'Courier New', Courier, monospace; font-weight: 600; }
    .stTabs [aria-selected="true"] { background: #2a9d8f !important; color: white !important; }
    hr { border-color: #b8e0d9; }
    
    /* Refresh status banner */
    .pipeline-status {
        background: linear-gradient(135deg, #d4f1f4, #b8e0d9);
        border-left: 4px solid #2a9d8f;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 8px 0;
        font-family: 'Courier New', Courier, monospace;
    }
    
    /* Logout button fix */
    [data-testid="stSidebar"] button {
        background-color: rgba(255, 255, 255, 0.15) !important;
        color: white !important;
        border: 1px solid rgba(255, 255, 255, 0.4) !important;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# BRAND COLORS
# ==============================================================================
COLORS = {
    'primary': '#2a9d8f', 'secondary': '#168aad', 'dark': '#1a6b5f',
    'light': '#d4f1f4', 'accent': '#e76f51', 'warning': '#f4a261',
    'class_a': '#2a9d8f', 'class_b': '#168aad', 'class_c': '#b8c5c3',
}

# ==============================================================================
# DATA DIRECTORY — all CSVs live here
# ==============================================================================
DATA_DIR = os.environ.get("URTHMAMA_DATA_DIR", "data")


# ==============================================================================
# DATA LOADING
# ==============================================================================
@st.cache_data
def load_data(data_dir: str, _cache_buster: str = ""):
    """Load all pipeline output CSVs. _cache_buster forces reload after refresh."""
    try:
        orders = pd.read_csv(os.path.join(data_dir, 'orders_clean_eda_ready.csv'))
        product_monthly = pd.read_csv(os.path.join(data_dir, 'product_monthly_performance.csv'))
        business_forecast = pd.read_csv(os.path.join(data_dir, 'business_forecast_18m.csv'))
        product_forecast = pd.read_csv(os.path.join(data_dir, 'product_level_forecast_12m.csv'))
        abc_class = pd.read_csv(os.path.join(data_dir, 'abc_classification.csv'))
        inventory = pd.read_csv(os.path.join(data_dir, 'inventory_optimization.csv'))

        orders['day'] = pd.to_datetime(orders['day'])
        product_monthly['year_month'] = pd.to_datetime(product_monthly['year_month'])
        business_forecast['year_month'] = pd.to_datetime(business_forecast['year_month'])
        product_forecast['year_month'] = pd.to_datetime(product_forecast['year_month'])

        # Variant-level forecast (optional — may not exist on first run)
        variant_forecast = None
        vf_path = os.path.join(data_dir, 'variant_allocated_forecast_12m.csv')
        if os.path.exists(vf_path):
            variant_forecast = pd.read_csv(vf_path)
            variant_forecast['year_month'] = pd.to_datetime(variant_forecast['year_month'])

        # Load metadata if available
        meta_path = os.path.join(data_dir, 'pipeline_metadata.json')
        metadata = None
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                metadata = json.load(f)

        return {
            'orders': orders,
            'product_monthly': product_monthly,
            'business_forecast': business_forecast,
            'product_forecast': product_forecast,
            'variant_forecast': variant_forecast,
            'abc_class': abc_class,
            'inventory': inventory,
            'metadata': metadata
        }
    except FileNotFoundError as e:
        return None


# Try loading existing data
data = load_data(DATA_DIR, _cache_buster=st.session_state.get("last_refresh", "init"))


# ==============================================================================
# SIDEBAR
# ==============================================================================
with st.sidebar:
    st.image("urth_mama_logo.png", width=180)
    st.markdown("### Analytics Dashboard")
    st.markdown(f"Welcome, **{st.session_state.get('user_name', '')}**")
    if st.button("Log out", use_container_width=True):
        for key in ["authenticated", "user_name", "user_id"]:
            st.session_state.pop(key, None)
        st.rerun()
    st.markdown("---")

    page = st.radio(
        "Navigate",
        ["Overview", "Product Classification", "Sales Forecast",
         "Inventory by Product", "New Products", "Promotions", "Customers", "Data Refresh"],
        label_visibility="collapsed"
    )

    st.markdown("---")

    if data is not None:
        st.markdown("##### Data Range")
        min_date = data['orders']['day'].min().strftime('%b %Y')
        max_date = data['orders']['day'].max().strftime('%b %Y')
        st.markdown(f"{min_date} → {max_date}")

        if data.get('metadata'):
            st.markdown(f"Last updated:")
            st.markdown(f"*{data['metadata'].get('last_run', 'N/A')[:16]}*")
            st.markdown(f"Orders: **{data['metadata'].get('total_orders', 'N/A'):,}**")
            st.markdown(f"Products forecasted: **{data['metadata'].get('products_forecasted', 'N/A')}**")
    else:
        st.markdown("No data loaded.")
        st.markdown("Go to **Data Refresh** to upload your Shopify export.")

    st.markdown("---")
    st.markdown("##### MSBA Capstone")
    st.markdown("Rabab Ali Swaidan")
    st.markdown("*May 2026*")


# ==============================================================================
# PAGE: DATA REFRESH (NEW)
# ==============================================================================
if page == "Data Refresh":
    st.markdown("# Refresh Forecasts")
    st.markdown("*Upload new sales data to refresh all forecasts and recommendations*")
    st.markdown("---")

    # Show current status
    if data is not None and data.get('metadata'):
        meta = data['metadata']
        st.markdown(
            f"""<div class="pipeline-status">
            <strong>Data:</strong> {meta.get('date_range', 'N/A')} &nbsp;|&nbsp;
            <strong>Orders:</strong> {meta.get('total_orders', 'N/A'):,} &nbsp;|&nbsp;
            <strong>Products forecasted:</strong> {meta.get('products_forecasted', 'N/A')} &nbsp;|&nbsp;
            <strong>Last updated:</strong> {meta.get('last_run', 'N/A')[:16]}
            </div>""",
            unsafe_allow_html=True
        )
    elif data is None:
        st.warning("No existing data found. Upload a Shopify export to get started.")

    st.markdown("<br>", unsafe_allow_html=True)

    # Instructions
    st.markdown("### How it works")
    st.markdown(
        "1. Export your sales data from **Shopify** → *Analytics → Reports → Sales*\n"
        "2. Upload the CSV file below\n"
        "3. Click **Run Pipeline** — forecasts, ABC classification, and inventory recommendations all refresh automatically\n"
        "4. Navigate to the other dashboard pages to see updated results"
    )

    st.markdown("---")

    # File uploader
    uploaded_file = st.file_uploader(
        "Upload Shopify Sales CSV",
        type=["csv"],
        help="This should be the raw CSV export from Shopify (e.g. 'Sales_Data_Urth Mama.csv')"
    )

    if uploaded_file is not None:
        # Preview the uploaded data
        try:
            preview_df = pd.read_csv(uploaded_file)
            uploaded_file.seek(0)  # reset for pipeline

            st.markdown("### Upload Preview")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Rows", f"{len(preview_df):,}")
            with col2:
                st.metric("Columns", f"{len(preview_df.columns)}")
            with col3:
                # Try to parse dates for range display
                try:
                    dates = pd.to_datetime(preview_df.iloc[:, 0], errors='coerce')
                    date_range = f"{dates.min().date()} → {dates.max().date()}"
                except:
                    date_range = "Detected"
                st.metric("Date Range", date_range)

            with st.expander("Show first 10 rows"):
                st.dataframe(preview_df.head(10), use_container_width=True, hide_index=True)

            # Validate expected columns exist
            cols_lower = [c.strip().lower().replace(" ", "_") for c in preview_df.columns]
            required = {"day", "product_title", "total_sales", "net_sales", "gross_sales"}
            # Accept either old or new Shopify column names
            has_orders = "orders" in cols_lower or "net_items_sold" in cols_lower
            has_order_id = "order_name" in cols_lower or "order_id" in cols_lower
            missing = required - set(cols_lower)
            if not has_orders:
                missing.add("orders or net_items_sold")
            if not has_order_id:
                missing.add("order_name or order_id")

            if missing:
                st.error(f"Missing expected columns: {', '.join(missing)}. "
                         "Make sure this is a Shopify sales export.")
            else:
                st.success("Column validation passed.")

                # Run pipeline button
                st.markdown("---")
                if st.button("Run Pipeline", type="primary", use_container_width=True):
                    # Save uploaded file temporarily
                    temp_path = os.path.join(DATA_DIR, "_temp_upload.csv")
                    os.makedirs(DATA_DIR, exist_ok=True)

                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                    # Run the pipeline with progress feedback
                    import importlib
                    import pipeline as pipeline_module
                    importlib.reload(pipeline_module)
                    from pipeline import run_full_pipeline

                    progress_bar = st.progress(0, text="Starting pipeline...")

                    try:
                        progress_bar.progress(10, text="Step 1/5: Cleaning raw data...")
                        results = run_full_pipeline(temp_path, output_dir=DATA_DIR)

                        progress_bar.progress(100, text="Pipeline complete!")

                        # Update session state to bust the data cache
                        st.session_state["last_refresh"] = datetime.now().isoformat()

                        # Clean up temp file
                        if os.path.exists(temp_path):
                            os.remove(temp_path)

                        # Show summary
                        meta = results["metadata"]
                        st.success(f"Done! All forecasts updated in {meta['elapsed_seconds']:.1f}s")

                        st.markdown("### Results Summary")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Orders Processed", f"{meta['total_orders']:,}")
                        with col2:
                            st.metric("Products Forecasted", meta['products_forecasted'])
                        with col3:
                            st.metric("Date Range", meta.get('date_range', 'N/A'))

                        st.info("Navigate to the other pages to explore updated forecasts, ABC analysis, and inventory recommendations.")

                        # Clear ALL cached data so dashboard loads fresh files
                        st.cache_data.clear()
                        st.session_state["last_refresh"] = datetime.now().isoformat()

                        # Force rerun to reload data
                        st.rerun()

                    except Exception as e:
                        progress_bar.empty()
                        st.error(f"Pipeline failed: {str(e)}")
                        st.exception(e)

        except Exception as e:
            st.error(f"Could not read the uploaded file: {str(e)}")

    # Manual refresh (rerun on existing data)
    st.markdown("---")
    st.markdown("### Alternative: Re-run with existing data")
    st.markdown("If you've manually placed a new Shopify CSV in the `data/` folder:")

    manual_csv = st.text_input(
        "Path to CSV file",
        value="",
        placeholder="e.g. data/Sales_Data_Urth Mama_2026.csv"
    )

    if manual_csv and st.button("Re-run Pipeline on This File"):
        if os.path.exists(manual_csv):
            import importlib
            import pipeline as pipeline_module
            importlib.reload(pipeline_module)
            from pipeline import run_full_pipeline
            with st.spinner("Running pipeline..."):
                try:
                    results = run_full_pipeline(manual_csv, output_dir=DATA_DIR)
                    st.cache_data.clear()
                    st.session_state["last_refresh"] = datetime.now().isoformat()
                    st.success(f"Done in {results['metadata']['elapsed_seconds']:.1f}s")
                    st.rerun()
                except Exception as e:
                    st.error(f"Pipeline failed: {str(e)}")
                    st.exception(e)
        else:
            st.error(f"File not found: {manual_csv}")


# ==============================================================================
# Guard: all other pages require data
# ==============================================================================
if page != "Data Refresh" and data is None:
    st.warning("No data available. Please go to **Data Refresh** and upload a Shopify export first.")
    st.stop()


# ==============================================================================
# PAGE: OVERVIEW
# ==============================================================================
if page == "Overview":
    st.markdown("# Business Overview")
    st.markdown("*Key performance indicators and trends*")
    st.markdown("---")

    # Filter to orders with actual product line items — excludes shipping-only
    # transactions. has_product flag is set by the pipeline.
    if 'has_product' in data['orders'].columns:
        product_orders = data['orders'][data['orders']['has_product'] == 1]
    else:
        product_orders = data['orders']

    total_orders = len(product_orders)
    avg_order_value = product_orders['order_total_sales'].mean()
    total_products = data['product_monthly']['product_title'].nunique()
    forecast_total = data['business_forecast']['forecast'].sum()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Orders", f"{total_orders:,}")
    with col2:
        st.metric("Avg Order Value", f"${avg_order_value:.2f}")
    with col3:
        st.metric("Active Products", f"{total_products}")

    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("18-Month Forecast", f"${forecast_total:,.0f}", delta=None)
        PRODUCT_COST_RATE = 0.387
        SALARY_18M = 1200 * 18
        SHIPPING_18M = 10000 * 1.5
        RENT_18M = 1800 * 1.5
        VIDEO_18M = 1000 * 1.5
        est_expenses = (forecast_total * PRODUCT_COST_RATE) + SALARY_18M + SHIPPING_18M + RENT_18M + VIDEO_18M
        est_profit = forecast_total - est_expenses
        st.markdown(f"**Estimated Profit: ${est_profit:,.0f}**")
        st.caption("Based on estimated product costs, salaries, shipping, rent, and video. Excludes taxes, packaging, returns, and import duties.")
    with col2:
        a_products = len(data['abc_class'][data['abc_class']['abc_class'] == 'A'])
        st.metric("Class A Products", f"{a_products}", delta=None)
    with col3:
        unique_customers = product_orders['customer_name'].nunique()
        st.metric("Unique Customers", f"{unique_customers:,}", delta=None)

    st.markdown("---")

    # Monthly Revenue Trend
    st.markdown("### Monthly Revenue Trend")
    monthly_revenue = product_orders.groupby(
        product_orders['day'].dt.to_period('M')
    )['order_total_sales'].sum().reset_index()
    monthly_revenue['day'] = monthly_revenue['day'].dt.to_timestamp()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=monthly_revenue['day'], y=monthly_revenue['order_total_sales'],
        mode='lines+markers', name='Revenue',
        line=dict(color=COLORS['primary'], width=3), marker=dict(size=6),
        fill='tozeroy', fillcolor='rgba(42, 157, 143, 0.1)'
    ))
    fig.update_layout(
        height=400, margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)', title='Revenue ($)'),
        font=dict(family='Courier New, Courier, monospace'), hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

    # ABC + Seasonal
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Revenue by Product Class")
        abc_summary = data['abc_class'].groupby('abc_class').agg(
            count=('product_title', 'count'), revenue=('total_revenue', 'sum')
        ).reset_index()
        fig = px.pie(abc_summary, values='revenue', names='abc_class', color='abc_class',
                     color_discrete_map={'A': COLORS['class_a'], 'B': COLORS['class_b'], 'C': COLORS['class_c']}, hole=0.4)
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20),
                          paper_bgcolor='rgba(0,0,0,0)', font=dict(family='Courier New, Courier, monospace'))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Seasonal Pattern")
        monthly_pattern = product_orders.groupby(product_orders['day'].dt.month)['order_total_sales'].mean().reset_index()
        monthly_pattern.columns = ['month', 'avg_sales']
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        monthly_pattern['month_name'] = monthly_pattern['month'].apply(lambda x: month_names[x-1])
        avg_line = monthly_pattern['avg_sales'].mean()
        colors = [COLORS['primary'] if v >= avg_line else COLORS['class_c'] for v in monthly_pattern['avg_sales']]

        fig = go.Figure()
        fig.add_trace(go.Bar(x=monthly_pattern['month_name'], y=monthly_pattern['avg_sales'], marker_color=colors))
        fig.add_hline(y=avg_line, line_dash="dash", line_color=COLORS['secondary'], annotation_text="Average")
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20),
                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                          xaxis=dict(showgrid=False),
                          yaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
                          font=dict(family='Courier New, Courier, monospace'))
        st.plotly_chart(fig, use_container_width=True)


# ==============================================================================
# PAGE: ABC ANALYSIS
# ==============================================================================
elif page == "Product Classification":
    st.markdown("# Product Classification")
    st.markdown("*Product classification by revenue contribution (Pareto analysis)*")
    st.markdown("---")

    st.markdown("""
**How products are classified:**

- **Class A** — Top products that together account for **80% of total revenue**. These are your highest-priority items: stock levels should be closely monitored and stockouts avoided.
- **Class B** — Products that account for the **next 15% of revenue** (80%–95% cumulative). Moderate priority: maintain adequate inventory without over-investing.
- **Class C** — The remaining products contributing the **last 5% of revenue**. Lower priority: consider reducing inventory or evaluating whether to continue carrying these items.
""")
    st.markdown("---")

    abc_summary = data['abc_class'].groupby('abc_class').agg(
        products=('product_title', 'count'), units=('total_units', 'sum')
    ).reset_index()
    total_products = abc_summary['products'].sum()

    col1, col2, col3 = st.columns(3)
    for col, abc in zip([col1, col2, col3], ['A', 'B', 'C']):
        row = abc_summary[abc_summary['abc_class'] == abc]
        if len(row) == 0:
            continue
        row = row.iloc[0]
        with col:
            color = {'A': '[A]', 'B': '[B]', 'C': '[C]'}[abc]
            st.markdown(f"### {color} Class {abc}")
            st.metric("Products", f"{row['products']} ({row['products']/total_products*100:.1f}%)")
            st.metric("Units Sold", f"{row['units']:,.0f}")

    st.markdown("---")
    st.markdown("### Pareto Chart")

    pareto_df = data['abc_class'].sort_values('total_revenue', ascending=False).reset_index(drop=True)
    pareto_df['cumulative_pct'] = pareto_df['total_revenue'].cumsum() / pareto_df['total_revenue'].sum() * 100
    pareto_df['product_index'] = range(1, len(pareto_df) + 1)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    colors = pareto_df['abc_class'].map({'A': COLORS['class_a'], 'B': COLORS['class_b'], 'C': COLORS['class_c']})
    fig.add_trace(go.Bar(x=pareto_df['product_index'], y=pareto_df['total_units'],
                         marker_color=colors, name='Units Sold',
                         hovertemplate='%{text}<br>Units: %{y:,.0f}<extra></extra>',
                         text=pareto_df['product_title']), secondary_y=False)
    fig.add_trace(go.Scatter(x=pareto_df['product_index'], y=pareto_df['cumulative_pct'],
                             mode='lines', name='Cumulative Revenue %',
                             line=dict(color=COLORS['accent'], width=3)), secondary_y=True)
    fig.add_hline(y=80, line_dash="dash", line_color=COLORS['primary'], annotation_text="80%", secondary_y=True)
    fig.add_hline(y=95, line_dash="dash", line_color=COLORS['warning'], annotation_text="95%", secondary_y=True)
    fig.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=20),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      font=dict(family='Courier New, Courier, monospace'),
                      legend=dict(orientation='h', yanchor='bottom', y=1.02),
                      xaxis=dict(title='Products (ranked by revenue)', showgrid=False))
    fig.update_yaxes(title_text="Units Sold", secondary_y=False, showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)')
    fig.update_yaxes(title_text="Cumulative Revenue %", secondary_y=True, showgrid=False, range=[0, 105])
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Class A Products (Top Performers)")

    # ── Load raw data to compute % sold on sale per product ──
    raw_path_abc = None
    for candidate in ["Sales_Data_Urth Mama.csv", "Sales_Data_Urth_Mama.csv",
                       os.path.join(DATA_DIR, "_temp_upload.csv")]:
        if os.path.exists(candidate):
            raw_path_abc = candidate
            break
    if raw_path_abc is None and os.path.exists(DATA_DIR):
        for f in os.listdir(DATA_DIR):
            if f.lower().startswith("sales_data") and f.endswith(".csv"):
                raw_path_abc = os.path.join(DATA_DIR, f)

    sale_pct_by_product = {}
    if raw_path_abc:
        @st.cache_data
        def compute_sale_pct(path, _bust=""):
            raw = pd.read_csv(path)
            raw.columns = raw.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
            raw["orders"] = pd.to_numeric(raw.get("orders", raw.get("net_items_sold", 0)), errors="coerce").fillna(0)
            raw["discount_value"] = pd.to_numeric(raw.get("discount_value", raw.get("discounts", 0)), errors="coerce").fillna(0)
            prod_raw = raw[raw["product_title"].notna() & (raw["orders"] > 0)].copy()
            # Exclude known wholesale customers — their discounts are B2B pricing, not promotions
            WHOLESALE_CUSTOMERS = [
                "safa awad", "wholesale ambefrul", "ambefrul", "samira",
                "rasha yassine", "fatima fadel", "imad play one",
                "wholesale trendy kids", "trendy kids"
            ]
            if "customer_name" in prod_raw.columns:
                prod_raw["_cust_lower"] = prod_raw["customer_name"].fillna("").str.strip().str.lower()
                prod_raw = prod_raw[~prod_raw["_cust_lower"].apply(
                    lambda c: any(w in c for w in WHOLESALE_CUSTOMERS)
                )]
            # An order line is "on sale" if discount_value > 0
            prod_raw["on_sale"] = prod_raw["discount_value"] > 0
            result = prod_raw.groupby("product_title").apply(
                lambda g: round(g["on_sale"].sum() / len(g) * 100, 1)
            ).to_dict()
            return result

        sale_pct_by_product = compute_sale_pct(raw_path_abc, _bust=st.session_state.get("last_refresh", ""))

    class_a = data['abc_class'][data['abc_class']['abc_class'] == 'A'][
        ['product_title', 'total_units', 'cumulative_pct']].copy()
    class_a['% Sold on Sale'] = class_a['product_title'].map(
        lambda p: f"{sale_pct_by_product.get(p, 0):.1f}%" if sale_pct_by_product else "N/A"
    )
    class_a.columns = ['Product', 'Units Sold', 'Cumulative %', '% Sold on Sale']
    class_a['Cumulative %'] = class_a['Cumulative %'].apply(lambda x: f"{x:.1f}%")
    st.dataframe(class_a, use_container_width=True, hide_index=True)
    st.caption("*% Sold on Sale* = share of line items for this product that had a discount applied.")

# ==============================================================================
# PAGE: SALES FORECAST
# ==============================================================================
elif page == "Sales Forecast":
    st.markdown("# Sales Forecast")
    st.markdown("*18-month retail revenue forecast with confidence intervals*")
    st.markdown("---")

    monthly_revenue = data['orders'].groupby(
        data['orders']['day'].dt.to_period('M')
    )['order_total_sales'].sum().reset_index()
    monthly_revenue['day'] = monthly_revenue['day'].dt.to_timestamp()
    monthly_revenue.columns = ['date', 'actual']

    forecast_df = data['business_forecast'].copy()
    
    # Check if new format with forecast_retail column exists
    if 'forecast_retail' in forecast_df.columns:
        # New format: use retail forecast for the chart
        forecast_df_display = forecast_df[['year_month', 'forecast_retail', 'lower_80', 'upper_80', 'lower_95', 'upper_95']].copy()
        forecast_df_display.columns = ['date', 'forecast', 'lower_80', 'upper_80', 'lower_95', 'upper_95']
        wholesale_total = forecast_df['wholesale_allowance'].sum()
        has_wholesale = True
    else:
        # Old format: use forecast column directly
        forecast_df_display = forecast_df.copy()
        forecast_df_display.columns = ['date', 'forecast', 'lower_80', 'upper_80', 'lower_95', 'upper_95']
        wholesale_total = 0
        has_wholesale = False

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=monthly_revenue['date'], y=monthly_revenue['actual'],
                             mode='lines+markers', name='Historical',
                             line=dict(color=COLORS['primary'], width=3), marker=dict(size=5)))

    fig.add_trace(go.Scatter(
        x=pd.concat([forecast_df_display['date'], forecast_df_display['date'][::-1]]),
        y=pd.concat([forecast_df_display['upper_95'], forecast_df_display['lower_95'][::-1]]),
        fill='toself', fillcolor='rgba(22, 138, 173, 0.15)',
        line=dict(color='rgba(0,0,0,0)'), name='95% PI', showlegend=True
    ))
    fig.add_trace(go.Scatter(
        x=pd.concat([forecast_df_display['date'], forecast_df_display['date'][::-1]]),
        y=pd.concat([forecast_df_display['upper_80'], forecast_df_display['lower_80'][::-1]]),
        fill='toself', fillcolor='rgba(22, 138, 173, 0.3)',
        line=dict(color='rgba(0,0,0,0)'), name='80% PI', showlegend=True
    ))
    fig.add_trace(go.Scatter(x=forecast_df_display['date'], y=forecast_df_display['forecast'],
                             mode='lines+markers', name='Retail Forecast',
                             line=dict(color=COLORS['accent'], width=3, dash='dash'),
                             marker=dict(size=6, symbol='diamond')))
    fig.add_shape(type="line", x0=monthly_revenue['date'].max(), x1=monthly_revenue['date'].max(),
                  y0=0, y1=1, yref="paper", line=dict(color="gray", width=2, dash="dot"))

    fig.update_layout(
        height=500, margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)', title=''),
        yaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)', title='Revenue ($)'),
        font=dict(family='Courier New, Courier, monospace'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

    # Wholesale note
    if has_wholesale:
        wholesale_monthly = int(wholesale_total / 18)
        wholesale_total_int = int(wholesale_total)
        st.info(
            f"Wholesale Revenue Note: In addition to the retail forecast shown above, "
            f"an estimated {wholesale_total_int:,} USD in wholesale revenue is expected "
            f"over the 18-month forecast period (approximately {wholesale_monthly:,} USD per month on average). "
            f"Wholesale orders are large B2B transactions with unpredictable timing, "
            f"so they are not assigned to specific months."
        )

    st.markdown("### Forecast Summary")
    retail_total = forecast_df_display['forecast'].sum()
    col1, col2, col3 = st.columns(3)
    with col1:
        if has_wholesale:
            st.metric("Retail Forecast (18 mo)", f"${retail_total:,.0f}")
        else:
            st.metric("Total Forecast (18 mo)", f"${retail_total:,.0f}")
        _fc_total = retail_total
        _expenses = (_fc_total * 0.387) + (1200*18) + (10000*1.5) + (1800*1.5) + (1000*1.5)
        _profit = _fc_total - _expenses
        st.markdown(f"**Estimated Profit: ${_profit:,.0f}**")
        st.caption("Based on estimated product costs, salaries, shipping, rent, and video. Excludes taxes, packaging, returns, and import duties.")
    with col2:
        if has_wholesale:
            st.metric("+ Wholesale Allowance", f"${wholesale_total:,.0f}")
        else:
            st.metric("Monthly Average", f"${forecast_df_display['forecast'].mean():,.0f}")
    with col3:
        if has_wholesale:
            st.metric("Combined Total", f"${retail_total + wholesale_total:,.0f}")
        else:
            st.metric("95% Range", f"${forecast_df_display['lower_95'].sum():,.0f} - ${forecast_df_display['upper_95'].sum():,.0f}")

    st.markdown("### Monthly Retail Forecast Table")
    display_df = forecast_df_display.copy()
    display_df['date'] = display_df['date'].dt.strftime('%Y-%m')
    display_df = display_df.round(0)
    display_df.columns = ['Month', 'Retail Forecast', 'Lower 80%', 'Upper 80%', 'Lower 95%', 'Upper 95%']

    def fmt(v):
        try:
            return f"${int(v):,}"
        except:
            return str(v)

    rows_html = "".join(
        f'<tr style="background:{"#f0f7f4" if i % 2 == 0 else "#ffffff"};">'
        f'<td style="padding:9px 14px;font-weight:600;color:#2a9d8f;">{row["Month"]}</td>'
        f'<td style="padding:9px 14px;text-align:right;font-weight:700;color:#1a6b5f;">{fmt(row["Retail Forecast"])}</td>'
        f'<td style="padding:9px 14px;text-align:right;color:#636e72;">{fmt(row["Lower 80%"])}</td>'
        f'<td style="padding:9px 14px;text-align:right;color:#636e72;">{fmt(row["Upper 80%"])}</td>'
        f'<td style="padding:9px 14px;text-align:right;color:#636e72;">{fmt(row["Lower 95%"])}</td>'
        f'<td style="padding:9px 14px;text-align:right;color:#636e72;">{fmt(row["Upper 95%"])}</td>'
        f'</tr>'
        for i, (_, row) in enumerate(display_df.iterrows())
    )

    header = (
        '<tr style="background:#2a9d8f;color:white;">'
        '<th style="padding:10px 14px;text-align:left;">Month</th>'
        '<th style="padding:10px 14px;text-align:right;">Retail Forecast</th>'
        '<th style="padding:10px 14px;text-align:right;">Lower 80%</th>'
        '<th style="padding:10px 14px;text-align:right;">Upper 80%</th>'
        '<th style="padding:10px 14px;text-align:right;">Lower 95%</th>'
        '<th style="padding:10px 14px;text-align:right;">Upper 95%</th>'
        '</tr>'
    )

    table_html = (
        '<div style="overflow-x:auto;border-radius:12px;border:1px solid #b8e0d9;">'
        '<table style="width:100%;border-collapse:collapse;font-family:Courier New,Courier,monospace;font-size:14px;">'
        f'<thead>{header}</thead>'
        f'<tbody>{rows_html}</tbody>'
        '</table></div>'
    )

    import streamlit.components.v1 as components
    components.html(table_html, height=len(display_df) * 38 + 60, scrolling=False)

    st.caption("Note: These figures represent gross sales revenue only. They do not account for product costs, shipping costs, marketing spend, or any other operating expenses. Profitability analysis requires cost data not included in this dataset.")




# ==============================================================================
# PAGE: INVENTORY BY PRODUCT
# ==============================================================================
elif page == "Inventory by Product":
    st.markdown("# Inventory by Product")
    st.markdown("*Product forecasts, classification, and inventory parameters*")
    st.markdown("---")

    vf = data.get('variant_forecast')
    has_variants = vf is not None

    # ── Filters ──
    all_products = sorted(data['abc_class']['product_title'].tolist())
    col_sel1, col_sel2 = st.columns([2, 2])

    with col_sel1:
        selected_product = st.selectbox("Select Product", options=all_products, index=0)

    # Variant filter
    selected_variant = None
    if has_variants:
        product_variants = sorted(
            vf[vf['product_title'] == selected_product]['product_variant_title'].unique().tolist()
        )
        has_multiple_variants = len(product_variants) > 1
        with col_sel2:
            if has_multiple_variants:
                variant_options = ["All Variants"] + product_variants
                selected_variant = st.selectbox("Filter by Variant (Color/Size)", options=variant_options, index=0)
                if selected_variant == "All Variants":
                    selected_variant = None
            else:
                st.markdown("<br>", unsafe_allow_html=True)
                if product_variants:
                    st.caption(f"Single variant: **{product_variants[0]}**")

    st.markdown("---")

    # ── Product info from ABC classification ──
    abc_info = data['abc_class'][data['abc_class']['product_title'] == selected_product]
    abc_class = abc_info['abc_class'].values[0] if len(abc_info) > 0 else 'N/A'
    total_rev = abc_info['total_revenue'].values[0] if len(abc_info) > 0 else 0
    total_units = abc_info['total_units'].values[0] if len(abc_info) > 0 else 0
    rev_pct = abc_info['revenue_pct'].values[0] if len(abc_info) > 0 else 0

    # ── Determine which forecast series to use ──
    if selected_variant and has_variants:
        product_fc = vf[
            (vf['product_title'] == selected_product) &
            (vf['product_variant_title'] == selected_variant)
        ][['year_month', 'forecast_units_variant']].copy()
        product_fc.columns = ['year_month', 'forecast_units']
        product_hist = data['product_monthly'][
            (data['product_monthly']['product_title'] == selected_product) &
            (data['product_monthly']['product_variant_title'] == selected_variant)
        ].groupby('year_month')['times_bought'].sum().reset_index()
        forecast_label = f"{selected_product} — {selected_variant}"
    else:
        product_fc = data['product_forecast'][
            data['product_forecast']['product_title'] == selected_product
        ].copy()
        product_hist = data['product_monthly'][
            data['product_monthly']['product_title'] == selected_product
        ].groupby('year_month')['times_bought'].sum().reset_index()
        forecast_label = selected_product

    product_hist.columns = ['date', 'actual']

    # ── KPI Row ──
    cc = {'A': '[A]', 'B': '[B]', 'C': '[C]'}.get(abc_class, '[?]')
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("ABC Class", f"{cc} {abc_class}")
    with col2:
        st.metric("Units Sold", f"{total_units:,.0f}")
    with col3:
        st.metric("12-Mo Forecast", f"{int(product_fc['forecast_units'].sum()):,} units")

    st.markdown("---")

    # ── Historical + Forecast Chart ──
    st.markdown("### Historical & Forecast")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=product_hist['date'], y=product_hist['actual'],
        mode='lines+markers', name='Historical',
        line=dict(color=COLORS['primary'], width=2), marker=dict(size=5)
    ))
    fig.add_trace(go.Scatter(
        x=product_fc['year_month'], y=product_fc['forecast_units'],
        mode='lines+markers', name='Forecast',
        line=dict(color=COLORS['accent'], width=2, dash='dash'),
        marker=dict(size=6, symbol='diamond')
    ))
    if len(product_hist) > 0:
        fig.add_shape(
            type="line",
            x0=product_hist['date'].max(), x1=product_hist['date'].max(),
            y0=0, y1=1, yref="paper",
            line=dict(color="gray", width=2, dash="dot")
        )
    fig.update_layout(
        height=400, margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)', title='Units'),
        font=dict(family='Courier New, Courier, monospace'), title=forecast_label,
        legend=dict(orientation='h', yanchor='bottom', y=1.02), hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Inventory Parameters ──
    # Check if inventory data has variant column (new format)
    has_inv_variant = 'Variant' in data['inventory'].columns
    
    if has_inv_variant and selected_variant:
        # Filter by product AND variant
        inv_info = data['inventory'][
            (data['inventory']['Product'] == selected_product) &
            (data['inventory']['Variant'] == selected_variant)
        ]
    elif has_inv_variant and selected_variant is None:
        # Show aggregated for all variants of this product
        inv_info = data['inventory'][data['inventory']['Product'] == selected_product]
    else:
        # Old format - product level only
        inv_info = data['inventory'][data['inventory']['Product'] == selected_product]
    
    if len(inv_info) > 0:
        st.markdown("### Inventory Parameters")
        
        if has_inv_variant and selected_variant is None and len(inv_info) > 1:
            # Multiple variants - show summary + table
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Avg Monthly Demand", f"{round(inv_info['Avg_Monthly_Demand'].sum())}")
            with col2:
                # Show dominant variability
                var_counts = inv_info['Demand_Variability'].value_counts()
                dominant_var = var_counts.index[0] if len(var_counts) > 0 else "N/A"
                st.metric("Dominant Variability", dominant_var)
            with col3:
                st.metric("Total Safety Stock", f"{inv_info['Safety_Stock'].sum():.0f} units")
            with col4:
                st.metric("Variants", f"{len(inv_info)}")
            
            # Show variant breakdown table
            st.markdown("#### Inventory by Variant")
            inv_display = inv_info[['Variant', 'Avg_Monthly_Demand', 'Demand_Variability', 'Safety_Stock', 'Reorder_Point']].copy()
            inv_display.columns = ['Variant', 'Avg Demand/Mo', 'Variability', 'Safety Stock', 'Reorder Point']
            for col in ['Avg Demand/Mo', 'Safety Stock', 'Reorder Point']:
                inv_display[col] = inv_display[col].round(0).astype(int)
            st.dataframe(inv_display, use_container_width=True, hide_index=True)
        else:
            # Single variant or old format
            inv_row = inv_info.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Avg Monthly Demand", f"{round(inv_row['Avg_Monthly_Demand'])}")
            with col2:
                st.metric("Demand Variability", inv_row['Demand_Variability'])
            with col3:
                st.metric("Safety Stock", f"{inv_row['Safety_Stock']:.0f} units")
            with col4:
                st.metric("Reorder Point", f"{inv_row['Reorder_Point']:.0f} units")
        
        # ── Wholesale demand note ──
        # Calculate wholesale % for this product from raw Shopify data
        WHOLESALE_CUSTOMERS = [
            'Safa Awad', 'Wholesale Ambefrul', 'Samira', 'Rasha Yassine',
            'Fatima Fadel', 'Imad Play One', 'Wholesale Trendy Kids'
        ]
        
        # Try to load raw data for wholesale calculation
        raw_path = None
        for candidate in ["Sales_Data_Urth Mama.csv", "Sales_Data_Urth_Mama.csv",
                          os.path.join(DATA_DIR, "Sales_Data_Urth Mama.csv"),
                          os.path.join(DATA_DIR, "Sales_Data_Urth_Mama.csv")]:
            if os.path.exists(candidate):
                raw_path = candidate
                break
        
        if raw_path:
            try:
                raw_df = pd.read_csv(raw_path)
                raw_df.columns = raw_df.columns.str.strip().str.lower().str.replace(" ", "_")
                # Handle different column names
                if 'net_items_sold' in raw_df.columns:
                    raw_df = raw_df.rename(columns={'net_items_sold': 'orders'})
                raw_df['orders'] = pd.to_numeric(raw_df['orders'], errors='coerce').fillna(0)
                
                product_rows = raw_df[raw_df['product_title'] == selected_product]
                if len(product_rows) > 0:
                    total_units = product_rows['orders'].sum()
                    wholesale_units = product_rows[product_rows['customer_name'].isin(WHOLESALE_CUSTOMERS)]['orders'].sum()
                    if total_units > 0:
                        wholesale_pct = (wholesale_units / total_units) * 100
                        if wholesale_pct >= 5:  # Only show if meaningful
                            st.caption(f"Note: {wholesale_pct:.0f}% of this product's historical demand comes from wholesale orders (timing unpredictable).")
            except Exception:
                pass  # Silently skip if can't calculate

    # ── Variant breakdown (only when no specific variant selected and product has variants) ──
    if selected_variant is None and has_variants:
        pv = vf[vf['product_title'] == selected_product]
        if len(pv['product_variant_title'].unique()) > 1:
            st.markdown("### Forecast by Variant")
            variant_totals = pv.groupby('product_variant_title')['forecast_units_variant'].sum()\
                .reset_index().sort_values('forecast_units_variant', ascending=False)
            variant_totals.columns = ['Variant', 'Forecast Units (12 mo)']

            fig2 = go.Figure(go.Bar(
                x=variant_totals['Forecast Units (12 mo)'],
                y=variant_totals['Variant'],
                orientation='h',
                marker_color=COLORS['secondary'],
                text=variant_totals['Forecast Units (12 mo)'].apply(lambda x: f"{x:,}"),
                textposition='outside'
            ))
            fig2.update_layout(
                height=max(200, len(variant_totals) * 42 + 60),
                margin=dict(l=20, r=80, t=10, b=20),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(title='Forecast Units', showgrid=True, gridcolor='rgba(42,157,143,0.1)'),
                yaxis=dict(autorange='reversed', showgrid=False),
                font=dict(family='Courier New, Courier, monospace')
            )
            st.plotly_chart(fig2, use_container_width=True)

    # ── 12-Month Forecast Table ──
    st.markdown("### 12-Month Forecast")
    fc_display = product_fc[['year_month', 'forecast_units']].copy()
    fc_display['year_month'] = fc_display['year_month'].dt.strftime('%Y-%m')
    fc_display['forecast_units'] = fc_display['forecast_units'].round(0).astype(int)
    fc_display.columns = ['Month', 'Forecast Units']
    st.dataframe(fc_display, use_container_width=True, hide_index=True)

    # ── All Products Summary Table ──
    st.markdown("---")
    st.markdown("### All Products Summary")
    st.markdown("*Filterable table of all products with inventory parameters*")
    
    col1, col2 = st.columns(2)
    with col1:
        abc_filter = st.multiselect("Filter by ABC Class", options=['A', 'B', 'C'], default=['A', 'B', 'C'])
    with col2:
        var_filter = st.multiselect("Filter by Demand Variability", options=['Low', 'Medium', 'High'], default=['Low', 'Medium', 'High'])

    # Check if inventory data has Variant column
    has_variant_col = 'Variant' in data['inventory'].columns
    
    # Merge forecast totals with inventory data
    forecast_totals = data['product_forecast'].groupby('product_title')['forecast_units'].sum().reset_index()
    forecast_totals.columns = ['Product', '12Mo_Forecast']
    
    all_products_df = data['inventory'].merge(forecast_totals, on='Product', how='left')
    all_products_df['12Mo_Forecast'] = all_products_df['12Mo_Forecast'].fillna(0)
    
    filtered_all = all_products_df[
        (all_products_df['ABC_Class'].isin(abc_filter)) &
        (all_products_df['Demand_Variability'].isin(var_filter))
    ].sort_values('Total_Revenue', ascending=False)

    if has_variant_col:
        display_all = filtered_all[['Product', 'Variant', 'ABC_Class', '12Mo_Forecast', 'Avg_Monthly_Demand', 
                                     'Demand_Variability', 'Safety_Stock', 'Reorder_Point']].copy()
        display_all.columns = ['Product', 'Variant', 'ABC', '12-Mo Forecast', 'Avg Demand/Mo', 'Variability', 'Safety Stock', 'Reorder Point']
    else:
        display_all = filtered_all[['Product', 'ABC_Class', '12Mo_Forecast', 'Avg_Monthly_Demand', 
                                     'Demand_Variability', 'Safety_Stock', 'Reorder_Point']].copy()
        display_all.columns = ['Product', 'ABC', '12-Mo Forecast', 'Avg Demand/Mo', 'Variability', 'Safety Stock', 'Reorder Point']
    
    for col in ['12-Mo Forecast', 'Avg Demand/Mo', 'Safety Stock', 'Reorder Point']:
        display_all[col] = display_all[col].round(0).astype(int)
    
    st.dataframe(display_all, use_container_width=True, hide_index=True)





# ==============================================================================
# PAGE: NEW PRODUCTS
# ==============================================================================
elif page == "New Products":
    st.markdown("# New Products")
    st.markdown("*Sales performance of products and new color ranges introduced in H2 2025*")
    st.markdown("---")

    NEW_PROD_RAW = None
    for candidate in ["Sales_Data_Urth Mama.csv", "Sales_Data_Urth_Mama.csv",
                       os.path.join(DATA_DIR, "_temp_upload.csv")]:
        if os.path.exists(candidate):
            NEW_PROD_RAW = candidate
            break
    if NEW_PROD_RAW is None and os.path.exists(DATA_DIR):
        for f in os.listdir(DATA_DIR):
            if f.lower().startswith("sales_data") and f.endswith(".csv"):
                NEW_PROD_RAW = os.path.join(DATA_DIR, f)

    if NEW_PROD_RAW is None:
        st.warning("Sales CSV not found.")
        st.stop()

    @st.cache_data
    def load_new_prod_perf(raw_path, _bust=""):
        df = pd.read_csv(raw_path)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        df["day"] = pd.to_datetime(df["day"], errors="coerce")
        df["orders"] = pd.to_numeric(df.get("orders", df.get("net_items_sold", 0)), errors="coerce").fillna(0)
        df = df[df["product_title"].notna() & (df["orders"] > 0)]
        return df

    raw = load_new_prod_perf(NEW_PROD_RAW, _bust=st.session_state.get("last_refresh", ""))

    # Newly introduced products with their intro dates
    NEW_PRODUCTS = [
        ("OmieBox Pastel",                      "2025-08-01", "New product"),
        ("Yumbox Tapas 5 Compartments (Large)", "2025-09-01", "New colors"),
        ("Yumbox Tapas 4 Compartments (Large)", "2025-09-01", "New colors"),
        ("MontiiCo 700ml Water Bottle",         "2025-10-01", "New colors"),
        ("MontiiCo 475ml Water Bottle",         "2025-10-01", "New colors"),
        ("MontiiCo 350ml Water Bottle",         "2025-10-01", "New colors"),
        ("MontiiCo Feast Lunchbox",             "2025-12-01", "New product"),
        ("MontiiCo Mini Food Jar",              "2025-12-01", "New product"),
        ("MontiiCo Food Jar 400ml",             "2025-12-01", "New product"),
    ]

    DATA_END = pd.Timestamp("2025-12-31")

    # Catalog-wide ranking over the same Aug-Dec window
    window_start = pd.Timestamp("2025-08-01")
    catalog_units = raw[raw["day"] >= window_start].groupby("product_title")["orders"].sum()
    catalog_rank  = catalog_units.rank(ascending=False, method="min")
    total_products = len(catalog_rank)

    # Build performance table
    rows = []
    for product, intro_str, ptype in NEW_PRODUCTS:
        intro_dt = pd.Timestamp(intro_str)
        psales = raw[(raw["product_title"] == product) & (raw["day"] >= intro_dt)]
        units = int(psales["orders"].sum())
        months_active = max(1, round((DATA_END - intro_dt).days / 30.44))
        avg_mo = round(units / months_active, 1)
        rank = int(catalog_rank.get(product, 0))
        rows.append({
            "Product": product,
            "Type": ptype,
            "Introduced": intro_dt.strftime("%b %Y"),
            "Months Active": int(months_active),
            "Total Units Sold": units,
            "Avg Units / Month": avg_mo,
            "Catalog Rank": f"{rank} of {total_products}" if rank > 0 else "No sales yet",
        })

    perf_df = pd.DataFrame(rows)
    active_df = perf_df[perf_df["Total Units Sold"] > 0]
    too_early = perf_df[perf_df["Total Units Sold"] == 0]

    # ── KPIs ──
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("New Products / Colors Tracked", f"{len(perf_df)}")
    with c2: st.metric("With Sales Data", f"{len(active_df)}")
    with c3: st.metric("Total Units Sold (H2 2025)", f"{perf_df['Total Units Sold'].sum():,}")

    st.markdown("---")

    # ── Velocity bar chart ──
    st.markdown("### Average Monthly Sales Velocity")
    st.markdown("*Units sold per month since introduction — comparable across products launched at different times*")

    vel_df = active_df.sort_values("Avg Units / Month", ascending=True)
    bar_colors = [COLORS["primary"] if r.startswith("1 ") or int(r.split(" of ")[0]) <= 10
                  else COLORS["secondary"] if int(r.split(" of ")[0]) <= 25
                  else COLORS["class_c"]
                  for r in vel_df["Catalog Rank"]]

    fig = go.Figure(go.Bar(
        x=vel_df["Avg Units / Month"],
        y=vel_df["Product"],
        orientation="h",
        marker_color=bar_colors,
        text=vel_df["Avg Units / Month"].apply(lambda x: f"{x:.1f} units/mo"),
        textposition="outside",
        customdata=vel_df[["Catalog Rank", "Months Active", "Total Units Sold"]].values,
        hovertemplate="<b>%{y}</b><br>%{x:.1f} units/month<br>Rank: %{customdata[0]}<br>Months active: %{customdata[1]}<br>Total sold: %{customdata[2]}<extra></extra>"
    ))
    fig.update_layout(
        height=max(280, len(vel_df) * 52 + 60),
        margin=dict(l=10, r=110, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="Avg Units per Month", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
        yaxis=dict(showgrid=False),
        font=dict(family="Courier New, Courier, monospace")
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Color = catalog rank: teal = top 10, blue = top 25, grey = outside top 25. Rank is among all 97 products sold Aug–Dec 2025.")

    st.markdown("---")

    # ── Monthly trend for selected product ──
    st.markdown("### Month-by-Month Sales Trend")

    selected = st.selectbox(
        "Select product",
        options=active_df["Product"].tolist(),
        index=0,
        key="newprod_trend"
    )

    if selected:
        intro_dt = pd.Timestamp(dict((p, d) for p, d, _ in NEW_PRODUCTS)[selected])
        monthly = (
            raw[(raw["product_title"] == selected) & (raw["day"] >= intro_dt)]
            .assign(year_month=lambda x: x["day"].dt.to_period("M").dt.to_timestamp())
            .groupby("year_month")["orders"].sum()
            .reset_index()
        )
        monthly.columns = ["month", "units"]

        rank_val = int(catalog_rank.get(selected, 0))
        avg_val = monthly["units"].mean()

        col_a, col_b = st.columns([2, 1])
        with col_a:
            fig2 = go.Figure(go.Bar(
                x=monthly["month"], y=monthly["units"],
                marker_color=COLORS["primary"],
                text=monthly["units"].apply(lambda x: str(int(x))),
                textposition="outside",
                hovertemplate="%{x|%b %Y}: %{y} units<extra></extra>"
            ))
            fig2.add_hline(
                y=avg_val, line_dash="dash", line_color=COLORS["secondary"],
                annotation_text=f"Avg: {avg_val:.1f}/mo"
            )
            fig2.update_layout(
                height=300, margin=dict(l=10, r=10, t=30, b=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=False),
                yaxis=dict(title="Units Sold", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                font=dict(family="Courier New, Courier, monospace"),
                title=selected
            )
            st.plotly_chart(fig2, use_container_width=True)

        with col_b:
            st.markdown("<br>", unsafe_allow_html=True)
            intro_label = pd.Timestamp(dict((p, d) for p, d, _ in NEW_PRODUCTS)[selected]).strftime("%b %Y")
            ptype_label = dict((p, t) for p, _, t in NEW_PRODUCTS)[selected]
            st.metric("Introduced", intro_label)
            st.metric("Type", ptype_label)
            st.metric("Avg / Month", f"{avg_val:.1f} units")
            st.metric("Catalog Rank", f"{rank_val} of {total_products}" if rank_val > 0 else "—")

    # ── Dec arrivals note ──
    if len(too_early) > 0:
        st.markdown("---")
        st.markdown("### Just Arrived — No Sales Data Yet")
        st.markdown("*These products were introduced in December 2025. Check back after Q1 2026 for meaningful performance data.*")
        for _, row in too_early.iterrows():
            st.markdown(f"- **{row['Product']}** ({row['Type']}, introduced {row['Introduced']})")



# ==============================================================================
# PAGE: PROMOTIONS & MARKETING IMPACT
# ==============================================================================
elif page == "Promotions":
    st.markdown("# Marketing & Promotions")
    st.markdown("*Three independent analyses: paid campaigns, content creation impact, and influencer shares*")
    st.markdown("---")

    # ── Shared helper: load & clean sales daily ──
    PROMO_RAW_PATH = None
    for candidate in ["Sales_Data_Urth Mama.csv", "Sales_Data_Urth_Mama.csv",
                       os.path.join(DATA_DIR, "_temp_upload.csv")]:
        if os.path.exists(candidate):
            PROMO_RAW_PATH = candidate
            break
    if PROMO_RAW_PATH is None and os.path.exists(DATA_DIR):
        for f in os.listdir(DATA_DIR):
            if f.lower().startswith("sales_data") and f.endswith(".csv"):
                PROMO_RAW_PATH = os.path.join(DATA_DIR, f)

    WHOLESALE_CUSTS = [
        "safa awad", "wholesale ambefrul", "ambefrul", "samira",
        "rasha yassine", "fatima fadel", "imad play one",
        "wholesale trendy kids", "trendy kids"
    ]

    @st.cache_data
    def load_daily_sales(raw_path, _bust=""):
        """
        Builds daily sales with a SARIMAX-based expected daily revenue baseline.
        Using the same SARIMAX(1,1,1)(0,1,1,12) + war dummies + maturity regressor
        as the main pipeline ensures the baseline accounts for seasonality, trend,
        and structural breaks — making lift comparisons defensible across all months.
        A simple rolling average cannot separate seasonal peaks from genuine lift.
        """
        import warnings
        warnings.filterwarnings("ignore")
        if raw_path is None or not os.path.exists(raw_path):
            return None
        df = pd.read_csv(raw_path)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        df["day"] = pd.to_datetime(df["day"], errors="coerce")
        df["net_sales"] = pd.to_numeric(df.get("net_sales", df.get("total_sales", 0)), errors="coerce").fillna(0)
        df["_cust"] = df.get("customer_name", pd.Series([""] * len(df))).fillna("").str.strip().str.lower()
        df = df[~df["_cust"].apply(lambda c: any(w in c for w in WHOLESALE_CUSTS))]
        df = df[df["product_title"].notna()]
        daily = df.groupby("day").agg(revenue=("net_sales", "sum"), orders=("order_name", "nunique")).reset_index()
        full_idx = pd.date_range(daily["day"].min(), daily["day"].max(), freq="D")
        daily = daily.set_index("day").reindex(full_idx, fill_value=0).reset_index()
        daily.rename(columns={"index": "day"}, inplace=True)

        # ── SARIMAX baseline ──
        # Build monthly series and fit same model as pipeline
        try:
            from statsmodels.tsa.statespace.sarimax import SARIMAX
            daily["ym"] = daily["day"].dt.to_period("M").dt.to_timestamp()
            monthly_ts = daily.groupby("ym")["revenue"].sum()
            monthly_ts.index = pd.DatetimeIndex(monthly_ts.index)
            monthly_ts = monthly_ts.asfreq("MS")

            # War imputation (same as pipeline)
            WAR_MONTHS_PROMO = ["2024-10-01", "2024-11-01"]
            for wm in WAR_MONTHS_PROMO:
                wm_ts = pd.Timestamp(wm)
                if wm_ts in monthly_ts.index:
                    same = monthly_ts[
                        (monthly_ts.index.month == wm_ts.month) &
                        (monthly_ts.index != wm_ts)
                    ]
                    if len(same) > 0:
                        monthly_ts[wm_ts] = same.mean()

            exog_promo = pd.DataFrame(index=monthly_ts.index)
            exog_promo["war_oct24"] = (monthly_ts.index == pd.Timestamp("2024-10-01")).astype(float)
            exog_promo["war_nov24"] = (monthly_ts.index == pd.Timestamp("2024-11-01")).astype(float)
            exog_promo["mature"]   = (monthly_ts.index >= pd.Timestamp("2023-07-01")).astype(float)

            model_promo = SARIMAX(
                monthly_ts, exog=exog_promo,
                order=(1,1,1), seasonal_order=(0,1,1,12),
                enforce_stationarity=False, enforce_invertibility=False
            ).fit(disp=False, maxiter=500)

            fitted = model_promo.fittedvalues.reset_index()
            fitted.columns = ["ym", "expected_monthly"]
            fitted["days_in_month"] = fitted["ym"].dt.daysinmonth
            fitted["expected_daily"] = fitted["expected_monthly"] / fitted["days_in_month"]
            daily = daily.merge(fitted[["ym", "expected_daily"]], on="ym", how="left")
        except Exception:
            # Fallback: 28-day rolling median if SARIMAX fails
            daily["expected_daily"] = daily["revenue"].shift(1).rolling(28, min_periods=14).median()

        daily["baseline"] = daily["expected_daily"]
        return daily

    daily_sales = load_daily_sales(PROMO_RAW_PATH, _bust=st.session_state.get("last_refresh", ""))

    if daily_sales is None:
        st.warning("Sales CSV not found. Place your Shopify export in the project folder.")
        st.stop()

    # ── File paths for marketing data (uploaded or in data/) ──
    URTH_CSV  = os.path.join(DATA_DIR, "Urth-Mama-Campaigns.csv")
    HIND_CSV  = os.path.join(DATA_DIR, "Hind-Amouri-Campaigns.csv")
    CC_XLS    = os.path.join(DATA_DIR, "content_creation.xlsx")
    IGS_XLS   = os.path.join(DATA_DIR, "Instagram_shares.xlsx")

    # ──────────────────────────────────────────────────────────
    # TAB LAYOUT
    # ──────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs([
        "Paid Campaigns",
        "Content Creation",
        "Influencer Shares"
    ])

    # ══════════════════════════════════════════════════════════
    # TAB 1: PAID CAMPAIGNS
    # ══════════════════════════════════════════════════════════
    with tab1:
        st.markdown("### Paid Campaign Performance")
        st.markdown("*Meta Ads spend across the Urth Mama and Hind Amouri accounts, and whether campaign months coincide with sales spikes*")

        with st.expander("Upload campaign CSVs (if not auto-detected)", expanded=False):
            urth_up = st.file_uploader("Urth Mama Campaigns CSV", type="csv", key="urth_tab")
            hind_up = st.file_uploader("Hind Amouri Campaigns CSV", type="csv", key="hind_tab")

        @st.cache_data
        def load_campaigns(urth_bytes, hind_bytes, _bust=""):
            import io
            frames = []
            for b, acct in [(urth_bytes, "Urth Mama"), (hind_bytes, "Hind Amouri")]:
                if b:
                    d = pd.read_csv(io.BytesIO(b))
                    d["account"] = acct
                    frames.append(d)
            if not frames:
                return None
            ig = pd.concat(frames, ignore_index=True)
            ig.columns = ig.columns.str.strip()
            ig["Ends"] = pd.to_datetime(ig["Ends"], errors="coerce")
            ig["Amount spent (USD)"] = pd.to_numeric(ig["Amount spent (USD)"], errors="coerce").fillna(0)
            ig["Impressions"] = pd.to_numeric(ig["Impressions"], errors="coerce").fillna(0)
            ig["Reach"] = pd.to_numeric(ig["Reach"], errors="coerce").fillna(0)
            ig["Results"] = pd.to_numeric(ig["Results"], errors="coerce").fillna(0)
            ig["year_month"] = ig["Ends"].dt.to_period("M").dt.to_timestamp()
            return ig

        def _bytes(upload, path):
            if upload is not None:
                return upload.read()
            if os.path.exists(path):
                with open(path, "rb") as f:
                    return f.read()
            return None

        urth_b = _bytes(urth_up, URTH_CSV)
        hind_b = _bytes(hind_up, HIND_CSV)
        camps = load_campaigns(urth_b, hind_b, _bust=st.session_state.get("last_refresh", ""))

        if camps is None:
            st.info("Upload the campaign CSVs above, or place them in the `data/` folder as `Urth-Mama-Campaigns.csv` and `Hind-Amouri-Campaigns.csv`.")
        else:
            total_spend    = camps["Amount spent (USD)"].sum()
            total_reach    = camps["Reach"].sum()
            total_results  = camps["Results"].sum()
            n_campaigns    = len(camps)
            hind_spend     = camps[camps["account"] == "Hind Amouri"]["Amount spent (USD)"].sum()
            urth_spend     = camps[camps["account"] == "Urth Mama"]["Amount spent (USD)"].sum()

            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric("Total Ad Spend", f"${total_spend:,.0f}")
            with c2: st.metric("Total Reach", f"{total_reach:,.0f}")
            with c3: st.metric("Total Results", f"{total_results:,.0f}")
            with c4: st.metric("Campaigns Run", f"{n_campaigns}")

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Spend by account ──
            acct_df = pd.DataFrame({
                "Account": ["Hind Amouri", "Urth Mama"],
                "Spend": [hind_spend, urth_spend]
            })

            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown("#### Spend by Account")
                fig = go.Figure(go.Bar(
                    x=acct_df["Account"], y=acct_df["Spend"],
                    marker_color=[COLORS["secondary"], COLORS["primary"]],
                    text=acct_df["Spend"].apply(lambda x: f"${x:,.0f}"),
                    textposition="outside"
                ))
                fig.update_layout(
                    height=300, margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    yaxis=dict(title="Spend ($)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                    xaxis=dict(showgrid=False),
                    font=dict(family="Courier New, Courier, monospace"), showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)

            with col_b:
                st.markdown("#### Monthly Spend Timeline")
                monthly = camps.groupby(["year_month", "account"])["Amount spent (USD)"].sum().reset_index()
                fig2 = go.Figure()
                for acct, color in [("Urth Mama", COLORS["primary"]), ("Hind Amouri", COLORS["secondary"])]:
                    sub = monthly[monthly["account"] == acct]
                    fig2.add_trace(go.Bar(x=sub["year_month"], y=sub["Amount spent (USD)"],
                                          name=acct, marker_color=color))
                fig2.update_layout(
                    barmode="stack", height=300, margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    yaxis=dict(title="Spend ($)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                    xaxis=dict(showgrid=False),
                    font=dict(family="Courier New, Courier, monospace"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02)
                )
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("---")
            st.markdown("#### Campaign Months vs. Sales")
            st.markdown("*Do months with higher ad spend coincide with higher daily revenue?*")

            # monthly sales
            ms = daily_sales.copy()
            ms["year_month"] = pd.to_datetime(ms["day"]).dt.to_period("M").dt.to_timestamp()
            monthly_sales = ms.groupby("year_month")["revenue"].sum().reset_index()
            monthly_spend_total = camps.groupby("year_month")["Amount spent (USD)"].sum().reset_index()

            merged = monthly_sales.merge(monthly_spend_total, on="year_month", how="left").fillna(0)
            merged["has_spend"] = merged["Amount spent (USD)"] > 0

            fig3 = make_subplots(specs=[[{"secondary_y": True}]])
            fig3.add_trace(go.Bar(
                x=merged["year_month"], y=merged["revenue"],
                name="Monthly Sales",
                marker_color=[COLORS["primary"] if h else COLORS["class_c"] for h in merged["has_spend"]],
                hovertemplate="%{x|%b %Y}<br>Sales: $%{y:,.0f}<extra></extra>"
            ), secondary_y=False)
            fig3.add_trace(go.Scatter(
                x=merged[merged["has_spend"]]["year_month"],
                y=merged[merged["has_spend"]]["Amount spent (USD)"],
                mode="markers", name="Ad Spend",
                marker=dict(color=COLORS["accent"], size=10, symbol="diamond"),
                hovertemplate="%{x|%b %Y}<br>Spend: $%{y:,.0f}<extra></extra>"
            ), secondary_y=True)
            fig3.update_layout(
                height=380, margin=dict(l=10, r=60, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Courier New, Courier, monospace"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                hovermode="x unified"
            )
            fig3.update_yaxes(title_text="Monthly Sales ($)", secondary_y=False,
                              showgrid=True, gridcolor="rgba(42,157,143,0.1)")
            fig3.update_yaxes(title_text="Ad Spend ($)", secondary_y=True, showgrid=False)
            st.plotly_chart(fig3, use_container_width=True)
            st.caption("Teal bars = months with active ad spend | Grey bars = no spend | Diamond markers show spend amount (right axis)")

            st.markdown("---")
            st.markdown("#### Top 10 Campaigns by Reach")
            top10 = camps.nlargest(10, "Reach")[["account", "Campaign name", "Amount spent (USD)", "Reach", "Results", "Ends"]].copy()
            top10["Amount spent (USD)"] = top10["Amount spent (USD)"].apply(lambda x: f"${x:,.2f}")
            top10["Reach"] = top10["Reach"].apply(lambda x: f"{x:,.0f}")
            top10["Results"] = top10["Results"].apply(lambda x: f"{x:,.0f}")
            top10["Ends"] = top10["Ends"].dt.strftime("%Y-%m-%d")
            top10.columns = ["Account", "Campaign", "Spend", "Reach", "Results", "End Date"]
            st.dataframe(top10, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════════════
    # TAB 2: CONTENT CREATION
    # ══════════════════════════════════════════════════════════
    with tab2:
        st.markdown("### Content Creation Impact")
        st.markdown("*Does daily revenue increase in the 7 days following a content post, compared to the SARIMAX seasonal expected value?*")

        with st.expander("Upload content creation dates (if not auto-detected)", expanded=False):
            cc_up = st.file_uploader("Content Creation Excel (.xlsx)", type="xlsx", key="cc_tab")

        @st.cache_data
        def load_content_dates(cc_bytes, _bust=""):
            import io
            if cc_bytes:
                df = pd.read_excel(io.BytesIO(cc_bytes))
            elif os.path.exists(CC_XLS):
                df = pd.read_excel(CC_XLS)
            else:
                return None
            df.columns = df.columns.str.strip()
            date_col = df.columns[0]
            df["date"] = pd.to_datetime(df[date_col], errors="coerce")
            return df.dropna(subset=["date"])

        cc_bytes = cc_up.read() if cc_up else None
        cc_df = load_content_dates(cc_bytes, _bust=st.session_state.get("last_refresh", ""))

        if cc_df is None:
            st.info("Upload the content creation Excel file above, or place it in `data/content_creation.xlsx`.")
        else:
            sales_end = daily_sales["day"].max()
            cc_df = cc_df[cc_df["date"] <= sales_end]

            # ── Per-event lift table ──
            POST_WINDOW = 7
            lift_rows = []
            for _, row in cc_df.iterrows():
                d = row["date"]
                post = daily_sales[(daily_sales["day"] > d) & (daily_sales["day"] <= d + pd.Timedelta(days=POST_WINDOW))]
                bl = daily_sales[daily_sales["day"] == d]["baseline"].values
                if len(post) == 0 or len(bl) == 0 or pd.isna(bl[0]) or bl[0] == 0:
                    continue
                avg_post = post["revenue"].mean()
                lift = (avg_post - bl[0]) / bl[0] * 100
                lift_rows.append({"date": d, "baseline_daily": bl[0], "avg_post_daily": avg_post, "lift_pct": lift})

            lift_df = pd.DataFrame(lift_rows)

            if len(lift_df) == 0:
                st.warning("No content dates overlap with the sales data range.")
            else:
                pct_positive = (lift_df["lift_pct"] > 0).mean() * 100
                median_lift  = lift_df["lift_pct"].median()
                avg_lift     = lift_df["lift_pct"].mean()
                n_events     = len(lift_df)

                c1, c2, c3, c4 = st.columns(4)
                with c1: st.metric("Posts Analysed", f"{n_events}")
                with c2: st.metric("Posts with Positive Lift", f"{pct_positive:.0f}%")
                with c3: st.metric("Median Lift (7-day avg)", f"{median_lift:+.1f}%")
                with c4: st.metric("Mean Lift", f"{avg_lift:+.1f}%")

                if median_lift > 5:
                    st.success(f"Content posts tend to coincide with a **+{median_lift:.0f}% lift** in daily revenue vs. the seasonal forecast baseline.")
                elif median_lift < -5:
                    st.info("Content posts do not show a clear positive sales effect in the 7-day window. This could mean the effect is longer-term, or that posts tend to happen during quieter periods.")
                else:
                    st.info("Content posts show a small or mixed effect on short-term daily revenue.")

                st.markdown("---")

                # ── Timeline: daily sales with post dates marked ──
                st.markdown("#### Daily Sales with Content Post Dates")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=daily_sales["day"], y=daily_sales["revenue"],
                    mode="lines", name="Daily Revenue",
                    line=dict(color=COLORS["primary"], width=1.5),
                    opacity=0.7
                ))
                fig.add_trace(go.Scatter(
                    x=daily_sales["day"], y=daily_sales["baseline"],
                    mode="lines", name="SARIMAX Expected",
                    line=dict(color=COLORS["class_c"], width=1.5, dash="dot")
                ))
                # Post markers
                post_sales = []
                for d in cc_df["date"]:
                    match = daily_sales[daily_sales["day"] == d]["revenue"]
                    post_sales.append(match.values[0] if len(match) > 0 else 0)
                fig.add_trace(go.Scatter(
                    x=cc_df["date"], y=post_sales,
                    mode="markers", name="Content Posted",
                    marker=dict(color=COLORS["accent"], size=9, symbol="triangle-up",
                                line=dict(color="white", width=1))
                ))
                fig.update_layout(
                    height=420, margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                    yaxis=dict(title="Revenue ($)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                    font=dict(family="Courier New, Courier, monospace"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    hovermode="x unified"
                )
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")
                st.markdown("#### Lift Distribution Across Posts")

                fig2 = go.Figure()
                colors_lift = [COLORS["primary"] if v >= 0 else COLORS["accent"] for v in lift_df["lift_pct"]]
                fig2.add_trace(go.Bar(
                    x=lift_df["date"], y=lift_df["lift_pct"],
                    marker_color=colors_lift,
                    hovertemplate="%{x|%b %d %Y}<br>Lift: %{y:+.1f}%<extra></extra>"
                ))
                fig2.add_hline(y=0, line_color="gray", line_width=1)
                fig2.add_hline(y=lift_df["lift_pct"].median(), line_dash="dash",
                               line_color=COLORS["secondary"], annotation_text=f"Median: {median_lift:+.1f}%")
                fig2.update_layout(
                    height=320, margin=dict(l=10, r=10, t=30, b=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(showgrid=False),
                    yaxis=dict(title="Revenue Lift (%)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                    font=dict(family="Courier New, Courier, monospace")
                )
                st.plotly_chart(fig2, use_container_width=True)
                st.caption("Each bar = one content post. Teal = revenue above SARIMAX seasonal expected in next 7 days. Orange = below expected.")

    # ══════════════════════════════════════════════════════════
    # TAB 3: INFLUENCER SHARES
    # ══════════════════════════════════════════════════════════
    with tab3:
        st.markdown("### Influencer Story Shares Impact")
        st.markdown("*Does daily revenue increase in the 7 days following an influencer share, compared to the SARIMAX seasonal expected value?*")

        with st.expander("Upload influencer shares file (if not auto-detected)", expanded=False):
            igs_up = st.file_uploader("Instagram Shares Excel (.xlsx)", type="xlsx", key="igs_tab")

        @st.cache_data
        def load_ig_shares(igs_bytes, _bust=""):
            import io
            if igs_bytes:
                df = pd.read_excel(io.BytesIO(igs_bytes))
            elif os.path.exists(IGS_XLS):
                df = pd.read_excel(IGS_XLS)
            else:
                return None
            df.columns = df.columns.str.strip()
            # Find date column — try known name first, then any column that parses as dates
            if "Sharing date" in df.columns:
                df["date"] = pd.to_datetime(df["Sharing date"], errors="coerce")
            else:
                # Try each column until one parses as dates
                date_col = None
                for col in df.columns:
                    parsed = pd.to_datetime(df[col], errors="coerce")
                    if parsed.notna().sum() > len(df) * 0.5:
                        date_col = col
                        break
                if date_col:
                    df["date"] = pd.to_datetime(df[date_col], errors="coerce")
                else:
                    return None
            # Find influencer column
            if "User" in df.columns:
                df["influencer"] = df["User"]
            else:
                # Use whichever column is not the date column
                non_date_cols = [c for c in df.columns if c not in ["date", "Sharing date"]]
                df["influencer"] = df[non_date_cols[0]] if non_date_cols else "Unknown"
            # Drop bad-year entries
            df = df[df["date"].dt.year < 2100].dropna(subset=["date"])
            return df

        igs_bytes = igs_up.read() if igs_up else None
        igs_df = load_ig_shares(igs_bytes, _bust=st.session_state.get("last_refresh", ""))

        if igs_df is None:
            st.info("Upload the Instagram shares Excel file above, or place it in `data/Instagram_shares.xlsx`.")
        else:
            sales_end = daily_sales["day"].max()
            igs_df = igs_df[igs_df["date"] <= sales_end]

            POST_WINDOW = 7
            lift_rows_ig = []
            for _, row in igs_df.iterrows():
                d = row["date"]
                post = daily_sales[(daily_sales["day"] > d) & (daily_sales["day"] <= d + pd.Timedelta(days=POST_WINDOW))]
                bl = daily_sales[daily_sales["day"] == d]["baseline"].values
                if len(post) == 0 or len(bl) == 0 or pd.isna(bl[0]) or bl[0] == 0:
                    continue
                avg_post = post["revenue"].mean()
                lift = (avg_post - bl[0]) / bl[0] * 100
                lift_rows_ig.append({
                    "date": d, "influencer": row["influencer"],
                    "baseline_daily": bl[0], "avg_post_daily": avg_post, "lift_pct": lift
                })

            lift_ig = pd.DataFrame(lift_rows_ig)

            if len(lift_ig) == 0:
                st.warning("No share dates overlap with the sales data range.")
            else:
                pct_pos_ig   = (lift_ig["lift_pct"] > 0).mean() * 100
                median_ig    = lift_ig["lift_pct"].median()
                avg_ig       = lift_ig["lift_pct"].mean()
                n_shares     = len(lift_ig)
                n_influencers = lift_ig["influencer"].nunique()

                c1, c2, c3, c4 = st.columns(4)
                with c1: st.metric("Shares Analysed", f"{n_shares}")
                with c2: st.metric("Influencers", f"{n_influencers}")
                with c3: st.metric("Shares with Positive Lift", f"{pct_pos_ig:.0f}%")
                with c4: st.metric("Median Lift (7-day avg)", f"{median_ig:+.1f}%")

                if median_ig > 5:
                    st.success(f"Influencer shares tend to coincide with a **+{median_ig:.0f}% lift** in daily revenue vs. the seasonal forecast baseline.")
                elif median_ig < -5:
                    st.info("Influencer shares don't show a clear short-term sales effect. The impact may be longer-term or brand-awareness oriented.")
                else:
                    st.info("Influencer shares show a mixed effect on short-term daily revenue.")

                st.markdown("---")

                # ── Timeline ──
                st.markdown("#### Daily Sales with Influencer Share Dates")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=daily_sales["day"], y=daily_sales["revenue"],
                    mode="lines", name="Daily Revenue",
                    line=dict(color=COLORS["primary"], width=1.5), opacity=0.7
                ))
                fig.add_trace(go.Scatter(
                    x=daily_sales["day"], y=daily_sales["baseline"],
                    mode="lines", name="SARIMAX Expected",
                    line=dict(color=COLORS["class_c"], width=1.5, dash="dot")
                ))
                share_sales = []
                for d in igs_df["date"]:
                    match = daily_sales[daily_sales["day"] == d]["revenue"]
                    share_sales.append(match.values[0] if len(match) > 0 else 0)
                fig.add_trace(go.Scatter(
                    x=igs_df["date"], y=share_sales,
                    mode="markers", name="Influencer Share",
                    marker=dict(color=COLORS["accent"], size=9, symbol="star",
                                line=dict(color="white", width=1)),
                    text=igs_df["influencer"],
                    hovertemplate="%{text}<br>%{x|%b %d %Y}<br>Revenue: $%{y:,.0f}<extra></extra>"
                ))
                fig.update_layout(
                    height=420, margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                    yaxis=dict(title="Revenue ($)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                    font=dict(family="Courier New, Courier, monospace"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    hovermode="x unified"
                )
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")

                col_left, col_right = st.columns(2)

                with col_left:
                    st.markdown("#### Lift per Share Event")
                    colors_ig = [COLORS["primary"] if v >= 0 else COLORS["accent"] for v in lift_ig["lift_pct"]]
                    fig2 = go.Figure(go.Bar(
                        x=lift_ig["date"], y=lift_ig["lift_pct"],
                        marker_color=colors_ig,
                        text=lift_ig["influencer"],
                        hovertemplate="%{text}<br>%{x|%b %d %Y}<br>Lift: %{y:+.1f}%<extra></extra>"
                    ))
                    fig2.add_hline(y=0, line_color="gray", line_width=1)
                    fig2.add_hline(y=median_ig, line_dash="dash",
                                   line_color=COLORS["secondary"],
                                   annotation_text=f"Median: {median_ig:+.1f}%")
                    fig2.update_layout(
                        height=320, margin=dict(l=10, r=10, t=30, b=10),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(showgrid=False),
                        yaxis=dict(title="Lift (%)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                        font=dict(family="Courier New, Courier, monospace")
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                with col_right:
                    st.markdown("#### Average Lift by Influencer")
                    by_inf = lift_ig.groupby("influencer").agg(
                        shares=("date", "count"),
                        median_lift=("lift_pct", "median"),
                        avg_lift=("lift_pct", "mean")
                    ).reset_index().sort_values("median_lift", ascending=True)

                    bar_colors = [COLORS["primary"] if v >= 0 else COLORS["accent"] for v in by_inf["median_lift"]]
                    fig3 = go.Figure(go.Bar(
                        x=by_inf["median_lift"],
                        y=by_inf["influencer"],
                        orientation="h",
                        marker_color=bar_colors,
                        text=by_inf.apply(lambda r: f"{r['shares']} share{'s' if r['shares']>1 else ''}", axis=1),
                        textposition="outside",
                        hovertemplate="%{y}<br>Median Lift: %{x:+.1f}%<extra></extra>"
                    ))
                    fig3.add_vline(x=0, line_color="gray", line_width=1)
                    fig3.update_layout(
                        height=320, margin=dict(l=10, r=60, t=10, b=10),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(title="Median 7-Day Lift (%)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                        yaxis=dict(showgrid=False),
                        font=dict(family="Courier New, Courier, monospace")
                    )
                    st.plotly_chart(fig3, use_container_width=True)
                    st.caption("Teal = positive lift | Orange = negative lift | Count = number of shares analysed")



# ==============================================================================
# PAGE: CUSTOMERS
# ==============================================================================
elif page == "Customers":
    st.markdown("# Customer Behavior")
    st.markdown("*Retention, order frequency, and customer value analysis*")
    st.markdown("---")

    if 'has_product' in data['orders'].columns:
        orders = data['orders'][data['orders']['has_product'] == 1].copy()
    else:
        orders = data['orders'].copy()
    orders['day'] = pd.to_datetime(orders['day'])

    # ── Build per-customer summary ──
    customer_summary = orders.groupby('customer_name').agg(
        total_orders=('order_name', 'nunique'),
        total_revenue=('order_total_sales', 'sum'),
        first_order=('day', 'min'),
        last_order=('day', 'max'),
        avg_order_value=('order_total_sales', 'mean'),
        discount_orders=('has_discount', 'sum'),
    ).reset_index()

    customer_summary['lifespan_days'] = (
        customer_summary['last_order'] - customer_summary['first_order']
    ).dt.days
    customer_summary['is_repeat'] = customer_summary['total_orders'] > 1
    customer_summary['discount_rate'] = (
        customer_summary['discount_orders'] / customer_summary['total_orders'] * 100
    ).round(1)
    customer_summary = customer_summary[customer_summary['customer_name'] != 'Unknown']

    total_customers = len(customer_summary)
    repeat_customers = customer_summary['is_repeat'].sum()
    repeat_rate = repeat_customers / total_customers * 100 if total_customers else 0
    avg_clv = customer_summary['total_revenue'].mean()
    avg_orders_per_customer = customer_summary['total_orders'].mean()
    one_time = total_customers - repeat_customers

    # ── KPI Row ──
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Customers", f"{total_customers:,}")
    with col2:
        st.metric("Repeat Customers", f"{repeat_customers:,}", delta=f"{repeat_rate:.1f}% of total")
    with col3:
        st.metric("Avg Customer Lifetime Value", f"${avg_clv:,.2f}")
    with col4:
        st.metric("One-Time Buyers", f"{one_time:,}", delta=f"{one_time/total_customers*100:.0f}% of customers", delta_color="inverse")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### New vs Repeat Customers")
        seg_df = pd.DataFrame({'Segment': ['One-Time', 'Repeat'], 'Customers': [one_time, repeat_customers]})
        fig = px.pie(seg_df, values='Customers', names='Segment', hole=0.4,
                     color_discrete_sequence=[COLORS['class_c'], COLORS['primary']])
        fig.update_layout(height=320, margin=dict(l=20, r=20, t=20, b=20),
                          paper_bgcolor='rgba(0,0,0,0)', font=dict(family='Courier New, Courier, monospace'))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### Order Frequency Distribution")
        freq_cap = customer_summary['total_orders'].clip(upper=10)
        freq_counts = freq_cap.value_counts().sort_index().reset_index()
        freq_counts.columns = ['Orders', 'Customers']
        freq_counts['label'] = freq_counts['Orders'].apply(lambda x: f"{int(x)}+" if x == 10 else str(int(x)))
        fig = go.Figure(go.Bar(x=freq_counts['label'], y=freq_counts['Customers'],
                               marker_color=COLORS['secondary']))
        fig.update_layout(height=320, margin=dict(l=20, r=20, t=20, b=20),
                          paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                          xaxis=dict(title='Number of Orders', showgrid=False),
                          yaxis=dict(title='Customers', showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
                          font=dict(family='Courier New, Courier, monospace'))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Top 10 Delivery Cities ──
    st.markdown("### Top 10 Delivery Cities")
    
    # Load raw data for shipping city
    try:
        raw_file = None
        for fname in ['Sales_Data_Urth_Mama.csv', 'Sales_Data_Urth Mama.csv']:
            # Check in DATA_DIR
            fpath = os.path.join(DATA_DIR, fname)
            if os.path.exists(fpath):
                raw_file = fpath
                break
            # Check in current directory
            if os.path.exists(fname):
                raw_file = fname
                break
        
        if raw_file:
            raw_data = pd.read_csv(raw_file)
            raw_data.columns = raw_data.columns.str.strip().str.lower().str.replace(' ', '_')
            
            # Filter to actual orders (not shipping-only rows)
            orders_geo = raw_data[raw_data['orders'] > 0].copy()
            
            # Aggregate by city
            city_stats = orders_geo.groupby('shipping_city').agg(
                total_orders=('order_name', 'nunique'),
                total_revenue=('total_sales', 'sum')
            ).reset_index()
            
            # Clean city names (title case) and combine duplicates
            city_stats['shipping_city'] = city_stats['shipping_city'].str.strip().str.title()
            
            # Standardize city names (Beyrouth → Beirut, etc.)
            city_name_map = {
                'Beyrouth': 'Beirut',
                'Bayrut': 'Beirut',
                'Saïda': 'Saida',
                'Sour': 'Tyre',
                'Jounieh': 'Jounieh',
                'Jbeil': 'Byblos',
            }
            city_stats['shipping_city'] = city_stats['shipping_city'].replace(city_name_map)
            
            city_stats = city_stats.groupby('shipping_city').agg(
                total_orders=('total_orders', 'sum'),
                total_revenue=('total_revenue', 'sum')
            ).reset_index().sort_values('total_orders', ascending=False)
            
            top_10 = city_stats.head(10)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = go.Figure(go.Bar(
                    x=top_10['total_orders'],
                    y=top_10['shipping_city'],
                    orientation='h',
                    marker_color=COLORS['primary'],
                    text=top_10['total_orders'],
                    textposition='outside'
                ))
                fig.update_layout(
                    height=400,
                    margin=dict(l=20, r=80, t=30, b=20),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(title='Number of Orders', showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
                    yaxis=dict(autorange='reversed', showgrid=False),
                    font=dict(family='Courier New, Courier, monospace'),
                    title=dict(text='By Orders', font=dict(size=14))
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                top_10_rev = city_stats.nlargest(10, 'total_revenue')
                fig = go.Figure(go.Bar(
                    x=top_10_rev['total_revenue'],
                    y=top_10_rev['shipping_city'],
                    orientation='h',
                    marker_color=COLORS['secondary'],
                    text=top_10_rev['total_revenue'].apply(lambda x: f"${x:,.0f}"),
                    textposition='outside'
                ))
                fig.update_layout(
                    height=400,
                    margin=dict(l=20, r=100, t=30, b=20),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(title='Revenue ($)', showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
                    yaxis=dict(autorange='reversed', showgrid=False),
                    font=dict(family='Courier New, Courier, monospace'),
                    title=dict(text='By Revenue', font=dict(size=14))
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Original Shopify file not found. Add 'Sales_Data_Urth_Mama.csv' to see geographic data.")
    except Exception as e:
        st.warning(f"Could not load geographic data: {e}")

    st.markdown("---")

    st.markdown("### New Customers per Month")
    new_by_month = (
        customer_summary
        .groupby(customer_summary['first_order'].dt.to_period('M'))
        .size().reset_index(name='new_customers')
    )
    new_by_month['first_order'] = new_by_month['first_order'].dt.to_timestamp()
    fig = go.Figure(go.Bar(x=new_by_month['first_order'], y=new_by_month['new_customers'],
                           marker_color=COLORS['primary']))
    fig.update_layout(height=320, margin=dict(l=20, r=20, t=20, b=20),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      xaxis=dict(showgrid=False),
                      yaxis=dict(title='New Customers', showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
                      font=dict(family='Courier New, Courier, monospace'), hovermode='x unified')
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.markdown("### Average Order Value: New vs Repeat Customers")
    orders_tagged = orders.merge(customer_summary[['customer_name', 'is_repeat']], on='customer_name', how='left')
    aov_by_month = (
        orders_tagged.groupby([orders_tagged['day'].dt.to_period('M'), 'is_repeat'])['order_total_sales']
        .mean().reset_index()
    )
    aov_by_month['day'] = aov_by_month['day'].dt.to_timestamp()
    aov_by_month['segment'] = aov_by_month['is_repeat'].map({True: 'Repeat', False: 'New'})
    fig = go.Figure()
    for segment, color in [('Repeat', COLORS['primary']), ('New', COLORS['accent'])]:
        seg = aov_by_month[aov_by_month['segment'] == segment]
        fig.add_trace(go.Scatter(x=seg['day'], y=seg['order_total_sales'],
                                 name=segment, mode='lines+markers',
                                 line=dict(color=color, width=2)))
    fig.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      xaxis=dict(showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
                      yaxis=dict(title='Avg Order Value ($)', showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
                      font=dict(family='Courier New, Courier, monospace'),
                      legend=dict(orientation='h', yanchor='bottom', y=1.02),
                      hovermode='x unified')
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.markdown("### Top Customers by Lifetime Value")
    top_customers = customer_summary.sort_values('total_revenue', ascending=False).head(20).copy()
    top_customers['first_order'] = top_customers['first_order'].dt.strftime('%Y-%m-%d')
    top_customers['last_order'] = top_customers['last_order'].dt.strftime('%Y-%m-%d')
    top_customers['total_revenue'] = top_customers['total_revenue'].apply(lambda x: f"${x:,.0f}")
    top_customers['avg_order_value'] = top_customers['avg_order_value'].apply(lambda x: f"${x:,.0f}")
    top_customers['discount_rate'] = top_customers['discount_rate'].apply(lambda x: f"{x:.1f}%")
    top_customers['is_repeat'] = top_customers['is_repeat'].map({True: 'Yes', False: '—'})
    top_customers = top_customers.rename(columns={
        'customer_name': 'Customer', 'total_orders': 'Orders',
        'total_revenue': 'Lifetime Revenue', 'avg_order_value': 'Avg Order Value',
        'first_order': 'First Order', 'last_order': 'Last Order',
        'discount_rate': 'Discount Rate', 'is_repeat': 'Repeat'
    })
    st.dataframe(
        top_customers[['Customer', 'Orders', 'Lifetime Revenue', 'Avg Order Value',
                       'First Order', 'Last Order', 'Discount Rate', 'Repeat']],
        use_container_width=True, hide_index=True
    )

# ==============================================================================
# FOOTER
# ==============================================================================
st.markdown("---")
st.markdown(
    "<p style='text-align: center; color: #5a9a8f; font-family: 'Courier New', Courier, monospace;'>"
    "Urth Mama Analytics Dashboard | MSBA Capstone Project | Rabab Ali Swaidan | May 2026"
    "</p>",
    unsafe_allow_html=True
)