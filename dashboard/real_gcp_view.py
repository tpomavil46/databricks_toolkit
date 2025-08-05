#!/usr/bin/env python3
"""
Real GCP Data Engineering Dashboard

Shows ONLY your actual Google Cloud billing data and usage metrics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """Show ONLY real GCP billing data for data engineering."""
    
    # Check if user wants to go back to analytics
    if st.session_state.get('show_analytics', False):
        st.session_state['show_analytics'] = False  # Reset for next time
        # This will be handled by the main app
        st.stop()
    
    st.set_page_config(
        page_title="GCP Data Engineering Dashboard",
        page_icon="☁️",
        layout="wide"
    )
    
    # Set up GCP
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    
    try:
        from dashboard.cloud_integrations import GoogleCloudIntegration, CostMonitor
        
        # Header with navigation
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.title("☁️ GCP Data Engineering Dashboard")
        
        with col2:
            if st.button("📊 Business Analytics", help="View business analytics dashboard"):
                st.session_state['show_analytics'] = True
                st.rerun()
        
        st.markdown("---")
        
        # Initialize GCP integration
        gcp = GoogleCloudIntegration()
        
        if not gcp.is_configured():
            st.error("❌ GCP not configured. Please set up your credentials.")
            return
        
        st.success(f"✅ Connected to GCP Project: **{gcp.project_id}**")
        
        # Get real billing data
        with st.spinner("🔍 Fetching your real GCP billing data..."):
            bigquery_data = gcp.get_bigquery_usage()
            storage_data = gcp.get_storage_usage()
        
        # Get comprehensive cost breakdown
        cost_monitor = CostMonitor()
        cost_breakdown = cost_monitor.get_total_cost_breakdown()
        
        # Calculate real costs by service
        gcp_costs = cost_breakdown[cost_breakdown['provider'] == 'gcp'] if not cost_breakdown.empty else pd.DataFrame()
        
        # Get the total cost (not sum of individual services)
        total_cost_row = gcp_costs[gcp_costs['service'] == 'total']
        total_real_cost = total_cost_row['cost'].sum() if not total_cost_row.empty else gcp_costs['cost'].sum()
        real_bq_cost = gcp_costs[gcp_costs['service'] == 'bigquery']['cost'].sum() if not gcp_costs.empty else 0
        real_storage_cost = gcp_costs[gcp_costs['service'] == 'storage']['cost'].sum() if not gcp_costs.empty else 0
        real_compute_cost = gcp_costs[gcp_costs['service'] == 'compute']['cost'].sum() if not gcp_costs.empty else 0
        real_functions_cost = gcp_costs[gcp_costs['service'] == 'functions']['cost'].sum() if not gcp_costs.empty else 0
        
        # Display real billing data
        st.header("💰 Real GCP Billing Data")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("💳 Total GCP Cost", f"${total_real_cost:.2f}")
        
        with col2:
            st.metric("📊 BigQuery Cost", f"${real_bq_cost:.2f}")
        
        with col3:
            st.metric("🗄️ Storage Cost", f"${real_storage_cost:.2f}")
        
        with col4:
            st.metric("⚙️ Compute Cost", f"${real_compute_cost:.2f}")
        
        # Show all services
        if not gcp_costs.empty:
            st.subheader("📋 All GCP Services")
            service_display = gcp_costs[['service', 'cost', 'is_real_data']].copy()
            service_display['cost'] = service_display['cost'].round(4)
            st.dataframe(service_display, use_container_width=True)
        
        # Show if this matches your GCP dashboard
        st.info(f"🔍 **Compare this to your GCP Console**: Your actual total cost is ${total_real_cost:.2f}")
        
        # BigQuery Usage Details
        st.header("📊 BigQuery Usage (Real Data)")
        
        if not bigquery_data.empty and real_bq_cost > 0:
            # Show BigQuery billing details
            st.subheader("📋 BigQuery Billing Details")
            bq_display = bigquery_data[['date', 'cost', 'query_count', 'bytes_processed']].copy()
            bq_display['bytes_processed_gb'] = bq_display['bytes_processed'] / (1024**3)
            bq_display['cost'] = bq_display['cost'].round(4)
            st.dataframe(bq_display, use_container_width=True)
            
            # BigQuery cost chart
            fig = px.line(bq_display, x='date', y='cost', title="BigQuery Cost Over Time")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 No BigQuery costs in this period")
        
        # Storage Usage Details
        st.header("🗄️ Cloud Storage Usage (Real Data)")
        
        if not storage_data.empty:
            st.subheader("📋 Storage Billing Details")
            storage_display = storage_data[['bucket_name', 'size_gb', 'object_count', 'storage_class', 'cost_per_month']].copy()
            st.dataframe(storage_display, use_container_width=True)
            
            # Storage cost chart
            if len(storage_data) > 0:
                fig = px.bar(
                    storage_data, 
                    x='bucket_name', 
                    y='cost_per_month',
                    title="Storage Cost by Bucket",
                    labels={'cost_per_month': 'Cost per Month ($)', 'bucket_name': 'Bucket Name'}
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("🗄️ No storage data available")
        
        # Data Engineering Metrics
        st.header("⚙️ Data Engineering Metrics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if not bigquery_data.empty:
                total_queries = bigquery_data['query_count'].sum()
                total_data_processed = bigquery_data['bytes_processed'].sum() / (1024**3)
                
                st.metric("🔍 Total Queries", f"{total_queries:,}")
                st.metric("📦 Data Processed", f"{total_data_processed:.2f} GB")
            else:
                st.metric("🔍 Total Queries", "0")
                st.metric("📦 Data Processed", "0 GB")
        
        with col2:
            if not storage_data.empty:
                total_storage_size = storage_data['size_gb'].sum()
                total_objects = storage_data['object_count'].sum()
                
                st.metric("💾 Total Storage", f"{total_storage_size:.2f} GB")
                st.metric("📁 Total Objects", f"{total_objects:,}")
            else:
                st.metric("💾 Total Storage", "0 GB")
                st.metric("📁 Total Objects", "0")
        
        # Cost Alert
        if total_real_cost == 0:
            st.success("🎉 **Cost Alert**: Your GCP costs are currently $0.00 - great job!")
        elif total_real_cost < 1:
            st.info(f"💡 **Cost Alert**: Your GCP costs are low at ${total_real_cost:.2f}")
        else:
            st.warning(f"⚠️ **Cost Alert**: Your GCP costs are ${total_real_cost:.2f}")
        
        # Footer
        st.markdown("---")
        st.markdown(
            """
            <div style='text-align: center; color: gray;'>
                <p>☁️ GCP Data Engineering Dashboard | Real billing data from your Google Cloud project</p>
            </div>
            """,
            unsafe_allow_html=True
        )
        
    except Exception as e:
        st.error(f"❌ Error loading GCP billing data: {e}")
        st.info("Please check your GCP credentials and project configuration.")

if __name__ == "__main__":
    main() 