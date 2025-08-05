#!/usr/bin/env python3
"""
Multi-Cloud Analytics Dashboard

A comprehensive Streamlit dashboard with Plotly visualizations for:
- Databricks Delta Live Tables pipelines
- Google Cloud cost monitoring
- Multi-cloud analytics
- Dynamic data source configuration
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as sp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import os

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import dashboard modules
try:
    from .config import config
    from .cloud_integrations import CostMonitor, DataSourceManager
except ImportError:
    # Fallback for when running as standalone script
    import sys
    sys.path.append(str(Path(__file__).parent))
    from config import config
    from cloud_integrations import CostMonitor, DataSourceManager

# Page configuration
st.set_page_config(
    page_title="Multi-Cloud Analytics Dashboard",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .pipeline-diagram {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e0e0e0;
    }
    .cloud-badge {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.75rem;
        font-weight: bold;
        margin: 0.125rem;
    }
    .databricks-badge { background-color: #ff6b35; color: white; }
    .gcp-badge { background-color: #4285f4; color: white; }
    .aws-badge { background-color: #ff9900; color: white; }
    .azure-badge { background-color: #0078d4; color: white; }
</style>
""", unsafe_allow_html=True)

def create_sample_data():
    """Create sample data for demonstration."""
    np.random.seed(42)
    
    # Sample analytics data with cloud sources
    categories = ['electronics', 'clothing', 'books', 'home', 'sports']
    product_categories = ['high_value', 'medium_value', 'low_value']
    cloud_providers = ['databricks', 'gcp', 'aws', 'azure']
    regions = ['us-east-1', 'us-west-1', 'eu-west-1', 'asia-pacific-1']
    
    data = []
    for i in range(100):
        cloud_provider = np.random.choice(cloud_providers)
        region = np.random.choice(regions)
        
        data.append({
            'id': i + 1,
            'name': f'Product {chr(65 + (i % 26))}',
            'category': np.random.choice(categories),
            'product_category': np.random.choice(product_categories),
            'total_revenue': np.random.uniform(10, 500),
            'avg_price': np.random.uniform(5, 200),
            'total_products': np.random.randint(1, 10),
            'high_value_products': np.random.randint(0, 5),
            'medium_value_products': np.random.randint(0, 5),
            'low_value_products': np.random.randint(0, 5),
            'valid_date_products': np.random.randint(1, 10),
            'valid_price_products': np.random.randint(1, 10),
            'high_value_revenue': np.random.uniform(0, 300),
            'medium_value_revenue': np.random.uniform(0, 200),
            'low_value_revenue': np.random.uniform(0, 100),
            'order_date': datetime.now() - timedelta(days=np.random.randint(0, 30)),
            'order_hour': np.random.randint(0, 24),
            'cloud_provider': cloud_provider,
            'region': region,
            'compute_cost': np.random.uniform(0.1, 5.0),
            'storage_cost': np.random.uniform(0.01, 0.5),
            'network_cost': np.random.uniform(0.05, 1.0),
            'total_cost': 0,  # Will be calculated
            'data_processed_gb': np.random.uniform(1, 100),
            'processing_time_minutes': np.random.uniform(1, 60)
        })
    
    df = pd.DataFrame(data)
    df['total_cost'] = df['compute_cost'] + df['storage_cost'] + df['network_cost']
    
    return df

def create_cloud_cost_data():
    """Create sample cloud cost data."""
    np.random.seed(42)
    
    # Generate cost data for different cloud providers
    providers = ['databricks', 'gcp', 'aws', 'azure']
    services = ['compute', 'storage', 'network', 'analytics', 'ml']
    
    cost_data = []
    for provider in providers:
        for service in services:
            for day in range(30):
                cost_data.append({
                    'date': datetime.now() - timedelta(days=day),
                    'provider': provider,
                    'service': service,
                    'cost': np.random.uniform(0.1, 10.0),
                    'usage_hours': np.random.uniform(1, 24),
                    'data_processed_gb': np.random.uniform(1, 100)
                })
    
    return pd.DataFrame(cost_data)

def create_pipeline_diagram():
    """Create a multi-cloud pipeline diagram using Plotly."""
    
    # Define pipeline stages with cloud providers
    stages = [
        ('Bronze\nIngestion', 'databricks'),
        ('Silver\nTransformation', 'gcp'),
        ('Gold\nAggregation', 'databricks'),
        ('Analytics\nDashboard', 'gcp')
    ]
    x_positions = [1, 2, 3, 4]
    
    # Create the pipeline diagram
    fig = go.Figure()
    
    # Add stage boxes with cloud provider colors
    colors = {
        'databricks': ('lightblue', 'darkblue'),
        'gcp': ('lightgreen', 'darkgreen'),
        'aws': ('lightcoral', 'darkred'),
        'azure': ('lightyellow', 'darkorange')
    }
    
    for i, ((stage, provider), x) in enumerate(zip(stages, x_positions)):
        bg_color, border_color = colors[provider]
        fig.add_shape(
            type="rect",
            x0=x-0.3, y0=0.2, x1=x+0.3, y1=0.8,
            fillcolor=bg_color,
            line=dict(color=border_color, width=2)
        )
        fig.add_annotation(
            x=x, y=0.5, text=stage,
            showarrow=False,
            font=dict(size=12, color="black")
        )
        # Add cloud provider badge
        fig.add_annotation(
            x=x, y=0.9, text=provider.upper(),
            showarrow=False,
            font=dict(size=10, color=border_color, weight="bold")
        )
    
    # Add arrows between stages
    for i in range(len(x_positions) - 1):
        fig.add_annotation(
            x=x_positions[i] + 0.4,
            y=0.5,
            xref="x",
            yref="y",
            axref="x",
            ayref="y",
            ax=x_positions[i+1] - 0.4,
            ay=0.5,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="gray"
        )
    
    # Add features for each stage
    features = [
        "• Auto Loader\n• Schema Inference\n• Multi-cloud",
        "• Data Quality\n• Business Logic\n• Cost Optimization",
        "• Real-time Analytics\n• Materialized Views\n• Performance Monitoring",
        "• Interactive Dashboards\n• Cost Tracking\n• Multi-cloud Integration"
    ]
    
    for i, (feature, x) in enumerate(zip(features, x_positions)):
        fig.add_annotation(
            x=x, y=-0.2, text=feature,
            showarrow=False,
            font=dict(size=10, color="gray"),
            align="center"
        )
    
    fig.update_layout(
        title="Multi-Cloud Delta Live Tables Pipeline Architecture",
        xaxis=dict(showgrid=False, showticklabels=False, range=[0.5, 4.5]),
        yaxis=dict(showgrid=False, showticklabels=False, range=[-0.5, 1.2]),
        width=1000,
        height=400,
        showlegend=False
    )
    
    return fig

def create_cost_analysis(df):
    """Create cloud cost analysis visualization."""
    
    # Cost by cloud provider
    cost_by_provider = df.groupby('cloud_provider')['total_cost'].sum().reset_index()
    
    fig = px.bar(
        cost_by_provider,
        x='cloud_provider',
        y='total_cost',
        title='Total Cost by Cloud Provider',
        color='cloud_provider',
        color_discrete_map={
            'databricks': '#ff6b35',
            'gcp': '#4285f4',
            'aws': '#ff9900',
            'azure': '#0078d4'
        }
    )
    
    fig.update_layout(
        xaxis_title="Cloud Provider",
        yaxis_title="Total Cost ($)",
        height=400
    )
    
    return fig

def create_cost_breakdown(df):
    """Create cost breakdown visualization."""
    
    # Cost breakdown by component
    cost_components = ['compute_cost', 'storage_cost', 'network_cost']
    component_names = ['Compute', 'Storage', 'Network']
    
    total_costs = []
    for component in cost_components:
        total_costs.append(df[component].sum())
    
    fig = px.pie(
        values=total_costs,
        names=component_names,
        title='Cost Breakdown by Component',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(height=400)
    
    return fig

def create_performance_metrics(df):
    """Create business performance metrics visualization."""
    
    # Business performance metrics based on real retail data
    perf_metrics = df.groupby('category').agg({
        'total_revenue': 'sum',
        'total_products': 'sum',
        'avg_price': 'mean'
    }).reset_index()
    
    fig = px.scatter(
        perf_metrics,
        x='total_products',
        y='total_revenue',
        size='avg_price',
        color='category',
        title='Business Performance by Category',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(
        xaxis_title="Total Products Sold",
        yaxis_title="Total Revenue ($)",
        height=400
    )
    
    return fig

def create_revenue_chart(df):
    """Create revenue visualization."""
    revenue_by_category = df.groupby('category')['total_revenue'].sum().reset_index()
    
    fig = px.bar(
        revenue_by_category,
        x='category',
        y='total_revenue',
        title='Total Revenue by Category',
        color='category',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(
        xaxis_title="Category",
        yaxis_title="Total Revenue ($)",
        height=400
    )
    
    return fig

def create_product_distribution(df):
    """Create product distribution visualization."""
    category_dist = df.groupby('product_category').agg({
        'total_products': 'sum',
        'total_revenue': 'sum'
    }).reset_index()
    
    fig = px.pie(
        category_dist,
        values='total_products',
        names='product_category',
        title='Product Distribution by Category',
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    
    fig.update_layout(height=400)
    
    return fig

def create_quality_metrics(df):
    """Create data quality metrics visualization."""
    quality_data = {
        'Metric': ['Valid Date Products', 'Valid Price Products', 'High Value', 'Medium Value', 'Low Value'],
        'Count': [
            df['valid_date_products'].sum(),
            df['valid_price_products'].sum(),
            df['high_value_products'].sum(),
            df['medium_value_products'].sum(),
            df['low_value_products'].sum()
        ]
    }
    
    quality_df = pd.DataFrame(quality_data)
    
    fig = px.bar(
        quality_df,
        x='Metric',
        y='Count',
        title='Data Quality Metrics',
        color='Count',
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(height=400)
    
    return fig

def create_revenue_trend(df):
    """Create revenue trend over time."""
    df['order_date'] = pd.to_datetime(df['order_date'])
    daily_revenue = df.groupby('order_date')['total_revenue'].sum().reset_index()
    
    fig = px.line(
        daily_revenue,
        x='order_date',
        y='total_revenue',
        title='Daily Revenue Trend',
        markers=True
    )
    
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Total Revenue ($)",
        height=400
    )
    
    return fig

def create_hourly_analysis(df):
    """Create hourly analysis visualization."""
    hourly_data = df.groupby('order_hour').agg({
        'total_revenue': 'sum',
        'total_products': 'sum'
    }).reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=hourly_data['order_hour'],
        y=hourly_data['total_revenue'],
        mode='lines+markers',
        name='Revenue',
        yaxis='y'
    ))
    
    fig.add_trace(go.Scatter(
        x=hourly_data['order_hour'],
        y=hourly_data['total_products'],
        mode='lines+markers',
        name='Products',
        yaxis='y2'
    ))
    
    fig.update_layout(
        title='Hourly Revenue and Product Analysis',
        xaxis_title="Hour of Day",
        yaxis=dict(title="Revenue ($)", side="left"),
        yaxis2=dict(title="Products", side="right", overlaying="y"),
        height=400
    )
    
    return fig

def main():
    """Main dashboard application."""
    
    # Check if user wants to see GCP dashboard
    if st.session_state.get('show_gcp', False):
        st.session_state['show_gcp'] = False  # Reset for next time
        # Import and run GCP dashboard
        from dashboard.real_gcp_view import main as gcp_main
        gcp_main()
        return
    
    # Header with navigation
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown('<h1 class="main-header">📊 Business Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    with col2:
        if st.button("☁️ GCP Data Engineering", help="View your GCP costs and data engineering metrics"):
            st.session_state['show_gcp'] = True
            st.rerun()
    
    # Sidebar
    st.sidebar.title("🎛️ Dashboard Controls")
    
    # Data source selection
    st.sidebar.subheader("📊 Data Source")
    available_sources = config.get_available_data_sources()
    data_source = st.sidebar.selectbox(
        "Select data source",
        options=available_sources,
        index=0
    )
    
    # Show data source info
    source_config = config.get_data_source_config(data_source)
    if source_config:
        st.sidebar.info(f"**{source_config.get('name', data_source)}**\n{source_config.get('description', '')}")
    
    # Cloud provider filter
    st.sidebar.subheader("☁️ Cloud Providers")
    selected_providers = st.sidebar.multiselect(
        "Select cloud providers",
        options=['databricks', 'aws', 'azure'],  # GCP handled in separate dashboard
        default=['databricks', 'aws']
    )
    
    # Date range filter
    st.sidebar.subheader("📅 Date Range")
    date_range = st.sidebar.date_input(
        "Select date range",
        value=(datetime.now() - timedelta(days=7), datetime.now()),
        max_value=datetime.now()
    )
    
    # Category filter
    st.sidebar.subheader("🏷️ Category Filter")
    selected_categories = st.sidebar.multiselect(
        "Select categories",
        options=['electronics', 'clothing', 'books', 'home', 'sports'],
        default=['electronics', 'clothing', 'books']
    )
    
    # Product category filter
    st.sidebar.subheader("💰 Product Category")
    selected_product_categories = st.sidebar.multiselect(
        "Select product categories",
        options=['high_value', 'medium_value', 'low_value'],
        default=['high_value', 'medium_value', 'low_value']
    )
    
    # Initialize managers
    data_manager = DataSourceManager()
    cost_monitor = CostMonitor()
    
    # Load data based on selected source
    df = data_manager.get_data_for_source(data_source)
    cost_df = cost_monitor.get_cost_trends()
    
    # Check if we have real data
    real_data_count = 0
    if 'is_real_data' in df.columns:
        real_data_count = df['is_real_data'].sum()
    
    # Data source indicator
    if real_data_count > 0:
        st.success(f"✅ Connected to real Databricks data ({real_data_count} real records)")
    else:
        st.info("📊 Business Analytics Dashboard - Sample data for demonstration")
    
    # Apply filters
    if selected_providers:
        df = df[df['cloud_provider'].isin(selected_providers)]
    if selected_categories:
        df = df[df['category'].isin(selected_categories)]
    if selected_product_categories:
        df = df[df['product_category'].isin(selected_product_categories)]
    
    # Main content
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Revenue",
            value=f"${df['total_revenue'].sum():,.2f}",
            delta=f"${df['total_revenue'].mean():.2f} avg"
        )
    
    with col2:
        st.metric(
            label="Total Cost",
            value=f"${df['total_cost'].sum():,.2f}",
            delta=f"${df['total_cost'].mean():.2f} avg"
        )
    
    with col3:
        st.metric(
            label="Data Processed",
            value=f"{df['data_processed_gb'].sum():,.1f} GB",
            delta=f"{df['data_processed_gb'].mean():.1f} GB avg"
        )
    
    with col4:
        st.metric(
            label="Avg Price",
            value=f"${df['avg_price'].mean():.2f}",
            delta=f"${df['avg_price'].std():.2f} std"
        )
    
    # Pipeline Diagram
    st.subheader("🔄 Multi-Cloud Pipeline Architecture")
    pipeline_fig = create_pipeline_diagram()
    st.plotly_chart(pipeline_fig, use_container_width=True)
    
    # Business Performance Metrics
    st.subheader("⚡ Business Performance")
    perf_fig = create_performance_metrics(df)
    st.plotly_chart(perf_fig, use_container_width=True)
    
    # Business Analytics
    st.subheader("📈 Business Analytics")
    col1, col2 = st.columns(2)
    
    with col1:
        revenue_fig = create_revenue_chart(df)
        st.plotly_chart(revenue_fig, use_container_width=True)
        
        quality_fig = create_quality_metrics(df)
        st.plotly_chart(quality_fig, use_container_width=True)
    
    with col2:
        distribution_fig = create_product_distribution(df)
        st.plotly_chart(distribution_fig, use_container_width=True)
        
        trend_fig = create_revenue_trend(df)
        st.plotly_chart(trend_fig, use_container_width=True)
    
    # Hourly Analysis
    st.subheader("⏰ Hourly Analysis")
    hourly_fig = create_hourly_analysis(df)
    st.plotly_chart(hourly_fig, use_container_width=True)
    
    # Data Table
    st.subheader("📋 Raw Business Data")
    
    display_df = df.head(20).copy()
    
    # Create HTML for the table
    html_rows = []
    for _, row in display_df.iterrows():
        html_row = f"""
        <tr>
            <td>{row['id']}</td>
            <td>{row['name']}</td>
            <td>{row['category']}</td>
            <td>{row['product_category']}</td>
            <td>${row['total_revenue']:.2f}</td>
            <td>${row['avg_price']:.2f}</td>
            <td>{row['total_products']}</td>
        </tr>
        """
        html_rows.append(html_row)
    
    html_table = f"""
    <table class="dataframe">
        <thead>
            <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Category</th>
                <th>Product Category</th>
                <th>Revenue</th>
                <th>Avg Price</th>
                <th>Products</th>
            </tr>
        </thead>
        <tbody>
            {''.join(html_rows)}
        </tbody>
    </table>
    """
    
    st.write(html_table, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
            <p>☁️ Multi-Cloud Analytics Dashboard | Built with Streamlit + Plotly | Delta Live Tables + Google Cloud</p>
            <p>🔄 Real-time data processing with Auto Loader, Materialized Views, and Cost Optimization</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main() 