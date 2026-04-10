"""
🌍 Urth Mama Analytics Dashboard
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
    page_icon="🌎",
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
        "<p style='text-align:center; font-family:Open Sans,sans-serif; "
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
    @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&family=Open+Sans:wght@300;400;600&display=swap');
    .stApp { background: linear-gradient(135deg, #f0f7f4 0%, #e8f4f8 100%); }
    h1, h2, h3 { font-family: 'Nunito', sans-serif !important; color: #2a9d8f !important; font-weight: 700 !important; }
    p, span, label, .stMarkdown { font-family: 'Open Sans', sans-serif !important; }
    [data-testid="metric-container"] {
        background: linear-gradient(145deg, #ffffff, #f0f7f4);
        border: 1px solid #b8e0d9; border-radius: 16px; padding: 20px;
        box-shadow: 0 4px 15px rgba(42, 157, 143, 0.1);
    }
    [data-testid="metric-container"] label { color: #5a9a8f !important; font-weight: 600 !important; text-transform: uppercase; font-size: 0.75rem !important; }
    [data-testid="metric-container"] [data-testid="stMetricValue"] { color: #1a6b5f !important; font-family: 'Nunito', sans-serif !important; font-size: 1.8rem !important; font-weight: 700 !important; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #1e6091 0%, #168aad 50%, #2a9d8f 100%); }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #ffffff !important; }
    [data-testid="stSidebar"] .stSelectbox label, [data-testid="stSidebar"] .stRadio label { color: #d4f1f4 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { background: #d4f1f4; border-radius: 8px; color: #1a6b5f; font-family: 'Nunito', sans-serif; font-weight: 600; }
    .stTabs [aria-selected="true"] { background: #2a9d8f !important; color: white !important; }
    hr { border-color: #b8e0d9; }
    
    /* Refresh status banner */
    .pipeline-status {
        background: linear-gradient(135deg, #d4f1f4, #b8e0d9);
        border-left: 4px solid #2a9d8f;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 8px 0;
        font-family: 'Open Sans', sans-serif;
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
         "Inventory by Product", "Promotions", "Customers", "Data Refresh"],
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
        st.markdown("⚠️ *No data loaded.*")
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

    total_revenue = product_orders['order_total_sales'].sum()
    total_orders = len(product_orders)
    avg_order_value = product_orders['order_total_sales'].mean()
    total_products = data['product_monthly']['product_title'].nunique()
    forecast_total = data['business_forecast']['forecast'].sum()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Revenue", f"${total_revenue:,.0f}")
    with col2:
        st.metric("Total Orders", f"{total_orders:,}")
    with col3:
        st.metric("Avg Order Value", f"${avg_order_value:.2f}")
    with col4:
        st.metric("Active Products", f"{total_products}")

    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("18-Month Forecast", f"${forecast_total:,.0f}", delta=None)
    with col2:
        st.metric("Monthly Avg (Forecast)", f"${data['business_forecast']['forecast'].mean():,.0f}")
    with col3:
        a_products = len(data['abc_class'][data['abc_class']['abc_class'] == 'A'])
        st.metric("Class A Products", f"{a_products}", delta=None)
    with col4:
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
        font=dict(family='Open Sans'), hovermode='x unified'
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
                          paper_bgcolor='rgba(0,0,0,0)', font=dict(family='Open Sans'))
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
                          font=dict(family='Open Sans'))
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
        products=('product_title', 'count'), revenue=('total_revenue', 'sum'), units=('total_units', 'sum')
    ).reset_index()
    total_products = abc_summary['products'].sum()
    total_revenue = abc_summary['revenue'].sum()

    col1, col2, col3 = st.columns(3)
    for col, abc in zip([col1, col2, col3], ['A', 'B', 'C']):
        row = abc_summary[abc_summary['abc_class'] == abc]
        if len(row) == 0:
            continue
        row = row.iloc[0]
        with col:
            color = {'A': '●', 'B': '●', 'C': '○'}[abc]
            st.markdown(f"### {color} Class {abc}")
            st.metric("Products", f"{row['products']} ({row['products']/total_products*100:.1f}%)")
            st.metric("Revenue", f"${row['revenue']:,.0f} ({row['revenue']/total_revenue*100:.1f}%)")

    st.markdown("---")
    st.markdown("### Pareto Chart")

    pareto_df = data['abc_class'].sort_values('total_revenue', ascending=False).reset_index(drop=True)
    pareto_df['cumulative_pct'] = pareto_df['total_revenue'].cumsum() / pareto_df['total_revenue'].sum() * 100
    pareto_df['product_index'] = range(1, len(pareto_df) + 1)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    colors = pareto_df['abc_class'].map({'A': COLORS['class_a'], 'B': COLORS['class_b'], 'C': COLORS['class_c']})
    fig.add_trace(go.Bar(x=pareto_df['product_index'], y=pareto_df['total_revenue'],
                         marker_color=colors, name='Revenue',
                         hovertemplate='%{text}<br>Revenue: $%{y:,.0f}<extra></extra>',
                         text=pareto_df['product_title']), secondary_y=False)
    fig.add_trace(go.Scatter(x=pareto_df['product_index'], y=pareto_df['cumulative_pct'],
                             mode='lines', name='Cumulative %',
                             line=dict(color=COLORS['accent'], width=3)), secondary_y=True)
    fig.add_hline(y=80, line_dash="dash", line_color=COLORS['primary'], annotation_text="80%", secondary_y=True)
    fig.add_hline(y=95, line_dash="dash", line_color=COLORS['warning'], annotation_text="95%", secondary_y=True)
    fig.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=20),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      font=dict(family='Open Sans'),
                      legend=dict(orientation='h', yanchor='bottom', y=1.02),
                      xaxis=dict(title='Products (ranked by revenue)', showgrid=False))
    fig.update_yaxes(title_text="Revenue ($)", secondary_y=False, showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)')
    fig.update_yaxes(title_text="Cumulative %", secondary_y=True, showgrid=False, range=[0, 105])
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Class A Products (Top Performers)")
    class_a = data['abc_class'][data['abc_class']['abc_class'] == 'A'][
        ['product_title', 'total_revenue', 'total_units', 'cumulative_pct']].copy()
    class_a.columns = ['Product', 'Revenue', 'Units Sold', 'Cumulative %']
    class_a['Revenue'] = class_a['Revenue'].apply(lambda x: f"${x:,.0f}")
    class_a['Cumulative %'] = class_a['Cumulative %'].apply(lambda x: f"{x:.1f}%")
    st.dataframe(class_a, use_container_width=True, hide_index=True)

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
        font=dict(family='Open Sans'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

    # Wholesale note
    if has_wholesale:
        wholesale_monthly = int(wholesale_total / 18)
        wholesale_total_int = int(wholesale_total)
        st.info(
            f"📦 Wholesale Revenue Note: In addition to the retail forecast shown above, "
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
        '<table style="width:100%;border-collapse:collapse;font-family:Open Sans,sans-serif;font-size:14px;">'
        f'<thead>{header}</thead>'
        f'<tbody>{rows_html}</tbody>'
        '</table></div>'
    )

    import streamlit.components.v1 as components
    components.html(table_html, height=len(display_df) * 38 + 60, scrolling=False)




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
    cc = {'A': '●', 'B': '●', 'C': '○'}.get(abc_class, '○')
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("ABC Class", f"{cc} {abc_class}")
    with col2:
        st.metric("Total Revenue", f"${total_rev:,.0f}")
    with col3:
        st.metric("Units Sold", f"{total_units:,.0f}")
    with col4:
        st.metric("Revenue Share", f"{rev_pct:.2f}%")
    with col5:
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
        font=dict(family='Open Sans'), title=forecast_label,
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
                font=dict(family='Open Sans')
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
# PAGE: PROMOTIONS ANALYSIS (streamlined)
# ==============================================================================
elif page == "Promotions":
    st.markdown("# Promotions & Discount Analysis")
    st.markdown("*Analysis of discount activity and promotional impact*")
    st.markdown("---")

    # We need line-item level data with discount info — rebuild from orders
    raw_path = None
    for candidate in ["Sales_Data_Urth Mama.csv", "Sales_Data_Urth_Mama.csv",
                       os.path.join(DATA_DIR, "_temp_upload.csv")]:
        if os.path.exists(candidate):
            raw_path = candidate
            break
    for f in os.listdir(DATA_DIR) if os.path.exists(DATA_DIR) else []:
        if f.lower().startswith("sales_data") and f.endswith(".csv"):
            raw_path = os.path.join(DATA_DIR, f)

    if raw_path is None:
        st.warning("Raw Shopify CSV not found. Place your Sales_Data file in the project folder to enable promotions analysis.")
        st.stop()

    @st.cache_data
    def load_promo_data(path, _bust=""):
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
        df["day"] = pd.to_datetime(df["day"], errors="coerce")
        for c in ["orders", "gross_sales", "net_sales", "total_sales", "discount_value"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df["discount_value"] = df["discount_value"].fillna(0)
        df["gross_sales"]    = df["gross_sales"].fillna(0)
        df["net_sales"]      = df["net_sales"].fillna(0)

        df["orders_qty"] = pd.to_numeric(df["orders"], errors="coerce").fillna(0)
        prod = df[df["product_title"].notna() & (df["orders_qty"] > 0)].copy()

        def categorize(name):
            if pd.isna(name):
                return "No Discount"
            n = str(name).strip().lower()
            if not n or n == "nan":
                return "No Discount"
            if any(k in n for k in ["gift", "freebie", "free ", "pr "]):
                return "Gift/Freebie"
            if any(k in n for k in ["influencer", "collab", "ambassador"]):
                return "Influencer/Collab"
            if any(k in n for k in ["wholesale", "retail"]):
                return "Wholesale"
            if any(k in n for k in ["warranty", "exchange", "return", "refund", "damage"]):
                return "Warranty/Exchange"
            if any(k in n for k in ["friend", "family", "employee"]):
                return "Friends & Family"
            if any(k in n for k in ["giveaway", "raffle", "contest"]):
                return "Giveaway"
            if any(k in n for k in ["instagram", "tag", "coupon"]):
                return "Social Media Promo"
            if any(k in n for k in ["sale", "offer", "promotion", "omieoff", "honoring"]):
                return "Sale/Promotion"
            if any(k in n for k in ["balance", "paid in", "old", "ordered befor", "ordered before", "exception", "custom", "discount", "20 off", "20off", "bundle", "special"]):
                return "Operational"
            return "Other"

        prod["discount_category"] = prod["discount_name"].apply(categorize)
        prod["year_month"] = prod["day"].dt.to_period("M").dt.to_timestamp()

        def pick_order_discount(g):
            named = g[(g["discount_name"].notna()) & (g["discount_value"] > 0)]
            if len(named) > 0:
                return named.iloc[0]["discount_value"]
            return 0.0

        order_disc_map = prod.groupby("order_name").apply(pick_order_discount)
        prod["order_discount"] = prod["order_name"].map(order_disc_map).fillna(0)

        first_disc_row = (
            prod[prod["order_discount"] > 0]
            .groupby("order_name")
            .apply(lambda g: g[(g["discount_name"].notna()) & (g["discount_value"] > 0)].index[0]
                   if len(g[(g["discount_name"].notna()) & (g["discount_value"] > 0)]) > 0
                   else g.index[0])
        )
        prod["is_first_disc_row"] = prod.index.isin(first_disc_row.values)
        prod["discount_deduped"] = prod.apply(
            lambda r: r["order_discount"] if r["is_first_disc_row"] else 0.0, axis=1
        )

        return prod

    promo = load_promo_data(raw_path, _bust=st.session_state.get("last_refresh", ""))

    # ── KPI Row ──
    total_orders      = promo["order_name"].nunique()
    discounted_orders = promo[promo["discount_category"] != "No Discount"]["order_name"].nunique()
    total_discount_given = promo["discount_deduped"].sum()
    marketing_cats = ["Sale/Promotion", "Influencer/Collab", "Social Media Promo", "Giveaway", "Wholesale"]
    marketing_rev  = promo[promo["discount_category"].isin(marketing_cats)]["net_sales"].sum()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Orders", f"{total_orders:,}")
    with col2:
        st.metric("Orders with Discounts", f"{discounted_orders:,}", delta=f"{discounted_orders/total_orders*100:.1f}%")
    with col3:
        st.metric("Total Discounts Given", f"${total_discount_given:,.0f}")
    with col4:
        st.metric("Promotional Revenue", f"${marketing_rev:,.0f}")

    st.markdown("---")

    # ── Discount Category Breakdown (consolidated into one chart) ──
    st.markdown("### Discount Breakdown by Purpose")

    group_map = {
        "Sale/Promotion":    "Marketing",
        "Influencer/Collab": "Marketing",
        "Social Media Promo":"Marketing",
        "Giveaway":          "Marketing",
        "Wholesale":         "Marketing",
        "Warranty/Exchange": "Operational / Warranty",
        "Operational":       "Operational / Warranty",
        "Gift/Freebie":      "Gifts & Freebies",
        "Friends & Family":  "Gifts & Freebies",
        "No Discount":       "No Discount",
        "Other":             "Other",
    }
    group_colors = {
        "Marketing":              COLORS['primary'],
        "Operational / Warranty": COLORS['secondary'],
        "Gifts & Freebies":       COLORS['warning'],
        "Other":                  COLORS['class_c'],
    }

    promo["discount_group"] = promo["discount_category"].map(group_map).fillna("Other")

    grp = promo[
        (promo["discount_group"] != "No Discount") &
        (promo["discount_deduped"] > 0)
    ].groupby("discount_group").agg(
        orders=("order_name", "nunique"),
        discount_given=("discount_deduped", "sum")
    ).reset_index().sort_values("discount_given", ascending=True)

    # Single consolidated horizontal bar showing both orders and discount value
    fig = make_subplots(rows=1, cols=2, subplot_titles=("By Order Count", "By Discount Value ($)"),
                        shared_yaxes=True, horizontal_spacing=0.12)
    fig.add_trace(go.Bar(
        x=grp["orders"], y=grp["discount_group"], orientation="h",
        marker_color=[group_colors.get(g, COLORS['class_c']) for g in grp["discount_group"]],
        text=grp["orders"].apply(lambda x: f"{x:,}"), textposition="outside",
        showlegend=False
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        x=grp["discount_given"], y=grp["discount_group"], orientation="h",
        marker_color=[group_colors.get(g, COLORS['class_c']) for g in grp["discount_group"]],
        text=grp["discount_given"].apply(lambda x: f"${x:,.0f}"), textposition="outside",
        showlegend=False
    ), row=1, col=2)
    fig.update_layout(
        height=300, margin=dict(l=20, r=100, t=40, b=20),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Open Sans")
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(42,157,143,0.1)")
    fig.update_yaxes(showgrid=False)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Monthly Promotional Impact ──
    st.markdown("### Monthly Promotional Impact")

    promo["is_marketing"] = promo["discount_category"].isin(marketing_cats)

    monthly_promo = promo.groupby("year_month").agg(
        total_revenue=("net_sales", "sum"),
        total_orders=("order_name", "nunique"),
        promo_revenue=("net_sales", lambda x: x[promo.loc[x.index, "is_marketing"]].sum()),
        promo_orders=("is_marketing", "sum"),
        discount_given=("discount_deduped", "sum")
    ).reset_index()

    monthly_promo["organic_revenue"] = monthly_promo["total_revenue"] - monthly_promo["promo_revenue"]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=monthly_promo["year_month"], y=monthly_promo["organic_revenue"],
        name="Organic Revenue", marker_color=COLORS['primary']
    ))
    fig.add_trace(go.Bar(
        x=monthly_promo["year_month"], y=monthly_promo["promo_revenue"],
        name="Promotional Revenue", marker_color=COLORS['accent']
    ))
    fig.add_trace(go.Scatter(
        x=monthly_promo["year_month"], y=monthly_promo["discount_given"],
        name="Discounts Given", mode="lines+markers",
        line=dict(color=COLORS['warning'], width=2, dash="dot"),
        yaxis="y2"
    ))

    fig.update_layout(
        barmode="stack", height=450,
        margin=dict(l=20, r=60, t=40, b=20),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False),
        yaxis=dict(title="Revenue ($)", showgrid=True, gridcolor='rgba(42, 157, 143, 0.1)'),
        yaxis2=dict(title="Discounts Given ($)", overlaying="y", side="right", showgrid=False),
        font=dict(family='Open Sans'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Top Promotional Events ──
    st.markdown("### Top Promotional Events")
    st.markdown("*Top 5 sale/promotion events by revenue generated*")

    sale_promos = promo[promo["discount_category"] == "Sale/Promotion"].copy()
    if len(sale_promos) > 0:
        sale_promos["clean_name"] = sale_promos["discount_name"].str.strip()
        event_table = sale_promos.groupby("clean_name").agg(
            Orders=("order_name", "nunique"),
            Start=("day", "min"),
            End=("day", "max"),
            Revenue=("net_sales", "sum"),
            Discount=("discount_deduped", "sum")
        ).sort_values("Revenue", ascending=False).head(5).reset_index()
        event_table.columns = ["Promotion", "Orders", "Start", "End", "Revenue", "Discount Given"]

        fig = go.Figure(go.Bar(
            x=event_table["Revenue"],
            y=event_table["Promotion"],
            orientation="h",
            marker_color=COLORS['accent'],
            text=event_table["Revenue"].apply(lambda x: f"${x:,.0f}"),
            textposition="outside",
            customdata=event_table[["Orders", "Discount Given"]],
            hovertemplate="<b>%{y}</b><br>Revenue: $%{x:,.0f}<br>Orders: %{customdata[0]}<br>Discount Given: $%{customdata[1]:,.0f}<extra></extra>"
        ))
        fig.update_layout(
            height=300, margin=dict(l=20, r=120, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="Revenue ($)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
            yaxis=dict(autorange="reversed", showgrid=False),
            font=dict(family="Open Sans")
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Promotional Lift (KPIs only — no redundant bar charts) ──
    st.markdown("### Promotional Lift")
    st.markdown("*Comparing average daily revenue and order value on days with active promotions vs. days without*")

    promo["date"] = promo["day"].dt.date
    daily = promo.groupby("date").agg(
        revenue=("net_sales", "sum"),
        orders=("order_name", "nunique"),
        has_promo=("discount_category", lambda x: int((x != "No Discount").any()))
    ).reset_index()
    daily["aov"] = daily["revenue"] / daily["orders"].replace(0, np.nan)

    promo_days     = daily[daily["has_promo"] == 1]
    no_promo_days  = daily[daily["has_promo"] == 0]

    avg_rev_promo    = promo_days["revenue"].mean()
    avg_rev_nopromo  = no_promo_days["revenue"].mean()
    avg_aov_promo    = promo_days["aov"].mean()
    avg_aov_nopromo  = no_promo_days["aov"].mean()
    rev_lift = (avg_rev_promo - avg_rev_nopromo) / avg_rev_nopromo * 100 if avg_rev_nopromo else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Avg Daily Rev — Promo Days", f"${avg_rev_promo:,.0f}")
    with col2:
        st.metric("Avg Daily Rev — No Promo", f"${avg_rev_nopromo:,.0f}")
    with col3:
        st.metric("Revenue Lift", f"+{rev_lift:.0f}%" if rev_lift > 0 else f"{rev_lift:.0f}%",
                  delta=f"${avg_rev_promo - avg_rev_nopromo:,.0f}/day")
    with col4:
        aov_diff = avg_aov_promo - avg_aov_nopromo
        st.metric("AOV Difference", f"${aov_diff:+,.0f}",
                  delta=f"Promo ${avg_aov_promo:,.0f} vs Full ${avg_aov_nopromo:,.0f}")

    st.markdown("---")

    # ── Instagram Campaign Performance (kept — optional/conditional) ──
    st.markdown("### Instagram Paid Campaign Performance")
    st.markdown("*Meta Ads campaigns run across the Urth Mama and Hind Amouri accounts*")

    URTH_CSV  = os.path.join(DATA_DIR, "Urth-Mama-Campaigns.csv")
    HIND_CSV  = os.path.join(DATA_DIR, "Hind-Amouri-Campaigns.csv")
    POSTS_XLS = os.path.join(DATA_DIR, "content_creation.xlsx")

    with st.expander("📂 Instagram data files (click to configure if not auto-detected)"):
        urth_upload  = st.file_uploader("Urth Mama Campaigns CSV",  type="csv",  key="urth_ig")
        hind_upload  = st.file_uploader("Hind Amouri Campaigns CSV", type="csv",  key="hind_ig")
        posts_upload = st.file_uploader("Content Creation dates (xlsx)", type="xlsx", key="posts_ig")

    @st.cache_data
    def load_ig_campaigns(urth_bytes, hind_bytes, posts_bytes, _bust=""):
        import io, pandas as pd

        def read_csv_bytes(b):
            return pd.read_csv(io.BytesIO(b))

        frames = []
        if urth_bytes:
            df = read_csv_bytes(urth_bytes)
            df["account"] = "Urth Mama"
            frames.append(df)
        if hind_bytes:
            df = read_csv_bytes(hind_bytes)
            df["account"] = "Hind Amouri"
            frames.append(df)
        if not frames:
            return None, None

        ig = pd.concat(frames, ignore_index=True)
        ig.columns = ig.columns.str.strip().str.lower().str.replace(" ", "_", regex=False).str.replace("_(usd)", "", regex=False)
        ig["amount_spent"] = pd.to_numeric(ig["amount_spent"], errors="coerce").fillna(0)
        ig["impressions"]  = pd.to_numeric(ig["impressions"],  errors="coerce").fillna(0)
        ig["reach"]        = pd.to_numeric(ig["reach"],        errors="coerce").fillna(0)
        ig["results"]      = pd.to_numeric(ig["results"],      errors="coerce").fillna(0)
        ig["cost_per_results"] = pd.to_numeric(ig["cost_per_results"], errors="coerce")
        ig["ends"] = pd.to_datetime(ig["ends"], errors="coerce")

        posts = None
        if posts_bytes:
            posts = pd.read_excel(io.BytesIO(posts_bytes))
            posts.columns = posts.columns.str.strip()
            posts["Content creation date"] = pd.to_datetime(posts["Content creation date"], errors="coerce")

        return ig, posts

    def _file_bytes(upload, path):
        if upload is not None:
            return upload.read()
        if os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()
        return None

    urth_bytes  = _file_bytes(urth_upload,  URTH_CSV)
    hind_bytes  = _file_bytes(hind_upload,  HIND_CSV)
    posts_bytes = _file_bytes(posts_upload, POSTS_XLS)

    ig, posts = load_ig_campaigns(
        urth_bytes, hind_bytes, posts_bytes,
        _bust=st.session_state.get("last_refresh", "")
    )

    if ig is None:
        st.info("Upload the Instagram campaign CSVs above (or place them in the `data/` folder as "
                "`Urth-Mama-Campaigns.csv` and `Hind-Amouri-Campaigns.csv`) to unlock this section.")
    else:
        # ── KPIs ──
        total_spend      = ig["amount_spent"].sum()
        total_reach      = ig["reach"].sum()
        total_impressions = ig["impressions"].sum()
        total_results    = ig["results"].sum()
        total_campaigns  = len(ig)
        avg_cpr          = ig["cost_per_results"].mean()

        k1, k2, k3, k4, k5 = st.columns(5)
        with k1: st.metric("Total Ad Spend",    f"${total_spend:,.0f}")
        with k2: st.metric("Total Reach",       f"{total_reach:,.0f}")
        with k3: st.metric("Total Impressions", f"{total_impressions:,.0f}")
        with k4: st.metric("Total Results",     f"{total_results:,.0f}")
        with k5: st.metric("Avg Cost/Result",   f"${avg_cpr:.2f}" if pd.notna(avg_cpr) else "—")

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Spend by Account")
            by_account = ig.groupby("account").agg(
                spend=("amount_spent", "sum"),
                campaigns=("campaign_name", "count"),
                reach=("reach", "sum")
            ).reset_index()
            fig = px.bar(
                by_account, x="account", y="spend", color="account",
                color_discrete_sequence=[COLORS['primary'], COLORS['secondary']],
                text=by_account["spend"].apply(lambda x: f"${x:,.0f}")
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(
                height=320, margin=dict(l=20, r=20, t=20, b=20),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(title="", showgrid=False),
                yaxis=dict(title="Spend ($)", showgrid=True, gridcolor="rgba(42,157,143,0.1)"),
                font=dict(family="Open Sans"), showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("#### Campaign Goal Mix")
            ig["goal"] = ig["result_indicator"].fillna("Unknown").apply(
                lambda x: (
                    "Reach"           if "reach" in str(x).lower() else
                    "Messaging/DM"    if "messaging" in str(x).lower() else
                    "Link Clicks"     if "link_click" in str(x).lower() else
                    "Profile Visits"  if "profile_visit" in str(x).lower() else
                    "Purchase"        if "purchase" in str(x).lower() else "Other"
                )
            )
            goal_summary = ig.groupby("goal").agg(
                spend=("amount_spent", "sum"),
                campaigns=("campaign_name", "count")
            ).reset_index()
            fig = px.pie(
                goal_summary, values="campaigns", names="goal", hole=0.4,
                color_discrete_sequence=[
                    COLORS['primary'], COLORS['secondary'], COLORS['accent'],
                    COLORS['warning'], COLORS['class_c'], "#8ecae6"
                ]
            )
            fig.update_layout(
                height=320, margin=dict(l=20, r=20, t=20, b=20),
                paper_bgcolor="rgba(0,0,0,0)", font=dict(family="Open Sans")
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        st.markdown("#### Top Campaigns by Reach")
        top_ig = ig.nlargest(15, "reach")[
            ["account", "campaign_name", "amount_spent", "reach", "impressions", "results", "cost_per_results", "ends"]
        ].copy()
        top_ig["amount_spent"]     = top_ig["amount_spent"].apply(lambda x: f"${x:,.2f}")
        top_ig["reach"]            = top_ig["reach"].apply(lambda x: f"{x:,.0f}")
        top_ig["impressions"]      = top_ig["impressions"].apply(lambda x: f"{x:,.0f}")
        top_ig["results"]          = top_ig["results"].apply(lambda x: f"{x:,.0f}")
        top_ig["cost_per_results"] = top_ig["cost_per_results"].apply(
            lambda x: f"${x:.2f}" if pd.notna(x) else "—"
        )
        top_ig["ends"] = top_ig["ends"].dt.strftime("%Y-%m-%d")
        top_ig.columns = ["Account", "Campaign", "Spend", "Reach", "Impressions", "Results", "Cost/Result", "End Date"]
        st.dataframe(top_ig, use_container_width=True, hide_index=True)


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
                          paper_bgcolor='rgba(0,0,0,0)', font=dict(family='Open Sans'))
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
                          font=dict(family='Open Sans'))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── Top 10 Delivery Cities ──
    st.markdown("### 📍 Top 10 Delivery Cities")
    
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
                    font=dict(family='Open Sans'),
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
                    font=dict(family='Open Sans'),
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
                      font=dict(family='Open Sans'), hovermode='x unified')
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
                      font=dict(family='Open Sans'),
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
    top_customers['is_repeat'] = top_customers['is_repeat'].map({True: '✓', False: '—'})
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
    "<p style='text-align: center; color: #5a9a8f; font-family: Open Sans;'>"
    "Urth Mama Analytics Dashboard | MSBA Capstone Project | Rabab Ali Swaidan | May 2026"
    "</p>",
    unsafe_allow_html=True
)
