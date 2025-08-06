"""
Dynamic Dashboard - Build Your Own Analytics Dashboard
Allows users to discover tables and create custom analytics through the UI
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any, Optional
import json
from datetime import datetime, timedelta
from pathlib import Path

# Import our existing modules
from cloud_integrations import DatabricksIntegration, GoogleCloudIntegration
from config import DashboardConfig

class DynamicDashboard:
    """Dynamic dashboard builder with table discovery and custom analytics"""
    
    def __init__(self):
        self.config = DashboardConfig()
        self.databricks = DatabricksIntegration()
        self.gcp = GoogleCloudIntegration()
        
    def discover_tables(self) -> List[Dict[str, Any]]:
        """Discover available tables in Databricks workspace"""
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.remote().getOrCreate()
            
            tables_info = []
            
            # Focus on Unity Catalog first (timpomaville)
            try:
                print("🔍 Discovering tables in timpomaville.default...")
                tables_df = spark.sql("SHOW TABLES IN timpomaville.default")
                for row in tables_df.collect():
                    table_name = row['tableName']
                    full_name = f"timpomaville.default.{table_name}"
                    display_name = f"default.{table_name}"
                    
                    tables_info.append({
                        'catalog': 'timpomaville',
                        'schema': 'default',
                        'table': table_name,
                        'full_name': full_name,
                        'display_name': display_name
                    })
                print(f"✅ Found {len(tables_info)} tables in timpomaville.default")
            except Exception as e:
                print(f"❌ Failed to get tables from timpomaville.default: {e}")
            
            # If no tables found, try information_schema approach
            if not tables_info:
                try:
                    print("🔍 Trying information_schema approach...")
                    info_df = spark.sql("SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_catalog = 'timpomaville' AND table_schema != 'information_schema'")
                    for row in info_df.collect():
                        catalog = row['table_catalog']
                        schema = row['table_schema']
                        table_name = row['table_name']
                        full_name = f"{catalog}.{schema}.{table_name}"
                        display_name = f"{schema}.{table_name}"
                        
                        tables_info.append({
                            'catalog': catalog,
                            'schema': schema,
                            'table': table_name,
                            'full_name': full_name,
                            'display_name': display_name
                        })
                    print(f"✅ Found {len(tables_info)} tables via information_schema")
                except Exception as e:
                    print(f"❌ information_schema approach failed: {e}")
            
            return tables_info
            
        except Exception as e:
            st.error(f"Error discovering tables: {e}")
            return []
    
    def discover_tables_controlled(self, selected_catalogs: List[str], selected_schemas: Optional[List[str]] = None, 
                                 include_info_schema: bool = False, max_tables: int = 100) -> List[Dict[str, Any]]:
        """Discover tables with user-controlled parameters"""
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.remote().getOrCreate()
            
            tables_info = []
            
            for catalog in selected_catalogs:
                try:
                    # Determine schemas to search
                    schemas_to_search = []
                    if selected_schemas and len(selected_catalogs) == 1:
                        schemas_to_search = selected_schemas
                    else:
                        # Get all schemas for this catalog
                        try:
                            schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
                            schemas_to_search = [row['namespace'] for row in schemas_df.collect()]
                        except Exception as e:
                            print(f"Could not get schemas for {catalog}: {e}")
                            schemas_to_search = ['default']
                    
                    for schema in schemas_to_search:
                        try:
                            # Skip information_schema unless explicitly requested
                            if schema == 'information_schema' and not include_info_schema:
                                continue
                            
                            # Get tables for this schema
                            tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
                            for row in tables_df.collect():
                                table_name = row['tableName']
                                full_name = f"{catalog}.{schema}.{table_name}"
                                display_name = f"{schema}.{table_name}"
                                
                                tables_info.append({
                                    'catalog': catalog,
                                    'schema': schema,
                                    'table': table_name,
                                    'full_name': full_name,
                                    'display_name': display_name
                                })
                                
                                # Check if we've reached the max limit
                                if len(tables_info) >= max_tables:
                                    print(f"Reached max tables limit ({max_tables})")
                                    return tables_info
                                    
                        except Exception as e:
                            print(f"Error getting tables for {catalog}.{schema}: {e}")
                            continue
                            
                except Exception as e:
                    print(f"Error processing catalog {catalog}: {e}")
                    continue
            
            return tables_info
        except Exception as e:
            st.error(f"Error in controlled table discovery: {e}")
            return []
    
    def get_table_data(self, catalog: str, schema: str, table: str, limit: int = 1000) -> pd.DataFrame:
        """Fetch data from a specific table"""
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.remote().getOrCreate()
            
            query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
            df = spark.sql(query).toPandas()
            return df
        except Exception as e:
            st.error(f"Error fetching data from {catalog}.{schema}.{table}: {e}")
            return pd.DataFrame()
    
    def get_table_data_with_date_filter(self, catalog: str, schema: str, table: str, limit: int = 1000, 
                                      start_date: Any = None, end_date: Any = None) -> pd.DataFrame:
        """Fetch data from a specific table with date filtering"""
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.remote().getOrCreate()
            
            # First, get the schema to find date columns
            schema_query = f"DESCRIBE {catalog}.{schema}.{table}"
            schema_df = spark.sql(schema_query).toPandas()
            
            # Look for date/timestamp columns
            date_columns = []
            for _, row in schema_df.iterrows():
                col_name = row['col_name']
                col_type = str(row['data_type']).lower()
                if 'date' in col_type or 'timestamp' in col_type:
                    date_columns.append(col_name)
            
            if date_columns:
                # Use the first date column found
                date_col = date_columns[0]
                query = f"""
                SELECT * FROM {catalog}.{schema}.{table} 
                WHERE {date_col} >= '{start_date}' AND {date_col} <= '{end_date}'
                LIMIT {limit}
                """
            else:
                # No date columns found, use regular query
                query = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
                st.warning("No date columns found in table. Loading all data.")
            
            df = spark.sql(query).toPandas()
            return df
        except Exception as e:
            st.error(f"Error fetching data with date filter from {catalog}.{schema}.{table}: {e}")
            return pd.DataFrame()
    
    def analyze_table_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze table schema and suggest analytics"""
        analysis: Dict[str, Any] = {
            'numeric_columns': [],
            'categorical_columns': [],
            'datetime_columns': [],
            'suggested_charts': []
        }
        
        for col in df.columns:
            col_type = str(df[col].dtype)
            
            if 'int' in col_type or 'float' in col_type:
                analysis['numeric_columns'].append(col)
            elif 'object' in col_type or 'category' in col_type:
                analysis['categorical_columns'].append(col)
            elif 'datetime' in col_type:
                analysis['datetime_columns'].append(col)
        
        # Suggest charts based on data types
        if len(analysis['numeric_columns']) > 0:
            analysis['suggested_charts'].append('histogram')
            analysis['suggested_charts'].append('box_plot')
            
        if len(analysis['categorical_columns']) > 0:
            analysis['suggested_charts'].append('bar_chart')
            analysis['suggested_charts'].append('pie_chart')
            
        if len(analysis['numeric_columns']) >= 2:
            analysis['suggested_charts'].append('scatter_plot')
            
        if len(analysis['datetime_columns']) > 0 and len(analysis['numeric_columns']) > 0:
            analysis['suggested_charts'].append('time_series')
            
        return analysis
    
    def create_chart(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: Optional[str] = None, 
                    color_col: Optional[str] = None, title: str = "") -> go.Figure:
        """Create a chart based on type and columns"""
        
        if chart_type == 'histogram':
            fig = px.histogram(df, x=x_col, title=title or f"Distribution of {x_col}")
            
        elif chart_type == 'bar_chart':
            if y_col:
                fig = px.bar(df, x=x_col, y=y_col, title=title or f"{y_col} by {x_col}")
            else:
                value_counts = df[x_col].value_counts()
                fig = px.bar(x=value_counts.index, y=value_counts.values, 
                           title=title or f"Count of {x_col}")
                
        elif chart_type == 'scatter_plot':
            fig = px.scatter(df, x=x_col, y=y_col, color=color_col, 
                           title=title or f"{y_col} vs {x_col}")
            
        elif chart_type == 'line_chart':
            fig = px.line(df, x=x_col, y=y_col, title=title or f"{y_col} over {x_col}")
            
        elif chart_type == 'pie_chart':
            value_counts = df[x_col].value_counts()
            fig = px.pie(values=value_counts.values, names=value_counts.index,
                        title=title or f"Distribution of {x_col}")
            
        elif chart_type == 'box_plot':
            fig = px.box(df, x=x_col, y=y_col, title=title or f"Box Plot: {y_col} by {x_col}")
            
        else:
            # Default to histogram
            fig = px.histogram(df, x=x_col, title=title or f"Distribution of {x_col}")
        
        fig.update_layout(height=400)
        return fig
    
    def create_architecture_diagram(self, table_info: Dict[str, Any]) -> go.Figure:
        """Create an architecture diagram showing the data flow"""
        
        # Determine the layer based on table name
        table_name = table_info['table'].lower()
        schema_name = table_info['schema'].lower()
        
        if 'bronze' in table_name or 'bronze' in schema_name:
            layer = "Bronze"
            layer_color = "#FF6B6B"  # Red
            description = "Raw data ingestion"
        elif 'silver' in table_name or 'silver' in schema_name:
            layer = "Silver"
            layer_color = "#4ECDC4"  # Teal
            description = "Cleaned and transformed data"
        elif 'gold' in table_name or 'gold' in schema_name:
            layer = "Gold"
            layer_color = "#45B7D1"  # Blue
            description = "Business-ready analytics"
        else:
            layer = "Custom"
            layer_color = "#96CEB4"  # Green
            description = "Custom table"
        
        # Create the architecture diagram
        fig = go.Figure()
        
        # Add nodes
        fig.add_trace(go.Scatter(
            x=[0.2, 0.5, 0.8],
            y=[0.5, 0.5, 0.5],
            mode='markers+text',
            marker=dict(
                size=[60, 80, 60],
                color=['#FF6B6B', layer_color, '#45B7D1'],
                line=dict(width=2, color='white')
            ),
            text=['Bronze', layer, 'Gold'],
            textposition="middle center",
            textfont=dict(size=14, color='white'),
            name="Data Layers"
        ))
        
        # Add arrows
        fig.add_trace(go.Scatter(
            x=[0.35, 0.65],
            y=[0.5, 0.5],
            mode='lines',
            line=dict(width=3, color='#666666'),
            showlegend=False
        ))
        
        # Add arrowheads
        fig.add_trace(go.Scatter(
            x=[0.35, 0.65],
            y=[0.5, 0.5],
            mode='markers',
            marker=dict(
                size=8,
                symbol='triangle-right',
                color='#666666'
            ),
            showlegend=False
        ))
        
        # Add table info
        fig.add_annotation(
            x=0.5,
            y=0.3,
            text=f"<b>Current Table:</b><br>{table_info['display_name']}<br><br><b>Layer:</b> {layer}<br><b>Description:</b> {description}",
            showarrow=False,
            font=dict(size=12),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="gray",
            borderwidth=1
        )
        
        # Update layout
        fig.update_layout(
            title=f"🏗️ Data Architecture - {layer} Layer",
            xaxis=dict(showgrid=False, showticklabels=False, range=[0, 1]),
            yaxis=dict(showgrid=False, showticklabels=False, range=[0, 1]),
            height=400,
            showlegend=False,
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        return fig
    
    def create_pipeline_architecture_diagram(self, pipeline_components: List[Dict[str, Any]]) -> go.Figure:
        """Create a dynamic pipeline architecture diagram based on user-defined components"""
        
        fig = go.Figure()
        
        if not pipeline_components:
            # Show empty pipeline
            fig.add_annotation(
                x=0.5,
                y=0.5,
                text="🏗️ <b>Empty Pipeline</b><br>Add components to build your pipeline",
                showarrow=False,
                font=dict(size=16, color='gray'),
                bgcolor="rgba(255,255,255,0.9)",
                bordercolor="lightgray",
                borderwidth=1
            )
        else:
            # Calculate positions dynamically
            spacing = 1.0 / (len(pipeline_components) + 1)
            
            for i, component in enumerate(pipeline_components):
                x_pos = spacing * (i + 1)
                y_pos = 0.5
                
                # Determine color based on layer type
                layer_type = component.get('layer_type', 'custom').lower()
                if 'bronze' in layer_type:
                    color = '#CD7F32'  # Bronze color
                elif 'silver' in layer_type:
                    color = '#C0C0C0'  # Silver color
                elif 'gold' in layer_type:
                    color = '#FFD700'  # Gold color
                else:
                    color = '#96CEB4'  # Custom color
                
                # Add component node
                fig.add_trace(go.Scatter(
                    x=[x_pos],
                    y=[y_pos],
                    mode='markers+text',
                    marker=dict(
                        size=80,
                        color=color,
                        line=dict(width=2, color='white')
                    ),
                    text=[component.get('layer_type', 'Custom').title()],
                    textposition="middle center",
                    textfont=dict(size=12, color='white'),
                    name=f"{component.get('layer_type', 'Custom')} Component"
                ))
                
                # Add table name annotation
                fig.add_annotation(
                    x=x_pos,
                    y=y_pos - 0.15,
                    text=f"<b>{component.get('table_name', 'Unknown')}</b><br>{component.get('description', '')}",
                    showarrow=False,
                    font=dict(size=10),
                    bgcolor="rgba(255,255,255,0.9)",
                    bordercolor="gray",
                    borderwidth=1
                )
                
                # Add dependency arrows
                if component.get('depends_on'):
                    # Find the position of the dependency
                    for j, dep_component in enumerate(pipeline_components):
                        if dep_component.get('table_name') == component['depends_on']:
                            dep_x = spacing * (j + 1)
                            dep_y = 0.5
                            
                            # Calculate arrow positions to avoid overlapping
                            # Start arrow further from source node
                            arrow_start_x = dep_x + 0.08
                            arrow_start_y = dep_y
                            
                            # End arrow further from target node
                            arrow_end_x = x_pos - 0.08
                            arrow_end_y = y_pos
                            
                            # Arrow from dependency TO current component (correct direction)
                            fig.add_trace(go.Scatter(
                                x=[arrow_start_x, arrow_end_x],
                                y=[arrow_start_y, arrow_end_y],
                                mode='lines',
                                line=dict(width=3, color='#666666'),
                                showlegend=False
                            ))
                            
                            # Arrowhead pointing TO current component
                            fig.add_trace(go.Scatter(
                                x=[arrow_end_x],
                                y=[arrow_end_y],
                                mode='markers',
                                marker=dict(
                                    size=8,
                                    symbol='triangle-right',
                                    color='#666666'
                                ),
                                showlegend=False
                            ))
                            break
        
        # Update layout
        fig.update_layout(
            title="🏗️ Pipeline Architecture",
            xaxis=dict(showgrid=False, showticklabels=False, range=[0, 1]),
            yaxis=dict(showgrid=False, showticklabels=False, range=[0, 1]),
            height=500,
            showlegend=False,
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        return fig
    
    def render_chart_builder(self):
        """Render the chart builder interface with pipeline integration"""
        st.header("📊 Chart Builder")
        st.markdown("Create charts from tables and view your pipeline architecture")
        
        # Show pipeline architecture if components exist
        if st.session_state.get('pipeline_components'):
            st.subheader("🏗️ Your Pipeline Architecture")
            arch_fig = self.create_pipeline_architecture_diagram(st.session_state['pipeline_components'])
            st.plotly_chart(arch_fig, use_container_width=True)
            
            # Dashboard customization
            st.subheader("🎨 Dashboard Customization")
            col1, col2 = st.columns(2)
            
            with col1:
                dashboard_name = st.text_input(
                    "Dashboard Name:",
                    placeholder="e.g., My Ecommerce Analytics",
                    key="dashboard_name"
                )
                
                dashboard_title = st.text_input(
                    "Dashboard Title:",
                    placeholder="e.g., Ecommerce Analytics Dashboard",
                    key="dashboard_title"
                )
            
            with col2:
                dashboard_description = st.text_area(
                    "Dashboard Description:",
                    placeholder="Describe your dashboard...",
                    key="dashboard_description",
                    height=80
                )
            
            # Save dashboard option
            st.subheader("💾 Dashboard Management")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 Save Dashboard") and dashboard_name:
                    # Save dashboard configuration (without DataFrame objects)
                    pipeline_components_clean = []
                    for comp in st.session_state['pipeline_components']:
                        clean_comp = {
                            'table_name': comp['table_name'],
                            'full_name': comp['full_name'],
                            'layer_type': comp['layer_type'],
                            'description': comp.get('description', ''),
                            'depends_on': comp.get('depends_on'),
                            'catalog': comp['catalog'],
                            'schema': comp['schema'],
                            'table': comp['table']
                            # Exclude 'data' field (DataFrame) to avoid JSON serialization error
                        }
                        pipeline_components_clean.append(clean_comp)
                    
                    dashboard_config = {
                        'name': dashboard_name,
                        'title': st.session_state.get('dashboard_title', dashboard_name),
                        'description': st.session_state.get('dashboard_description', ''),
                        'pipeline_components': pipeline_components_clean,
                        'created_at': str(pd.Timestamp.now()),
                        'tables': [comp['full_name'] for comp in st.session_state['pipeline_components']]
                    }
                    
                    # Save to file (simple JSON for now)
                    
                    dashboards_dir = Path(__file__).parent / "saved_dashboards"
                    dashboards_dir.mkdir(exist_ok=True)
                    
                    dashboard_file = dashboards_dir / f"{dashboard_name.replace(' ', '_').lower()}.json"
                    with open(dashboard_file, 'w') as f:
                        json.dump(dashboard_config, f, indent=2)
                    
                    st.success(f"✅ Dashboard '{dashboard_name}' saved!")
            
            with col2:
                if st.button("📋 Load Saved Dashboards"):
                    # List saved dashboards
                    dashboards_dir = Path(__file__).parent / "saved_dashboards"
                    if dashboards_dir.exists():
                        dashboard_files = list(dashboards_dir.glob("*.json"))
                        if dashboard_files:
                            st.write("**Saved Dashboards:**")
                            for dashboard_file in dashboard_files:
                                with open(dashboard_file, 'r') as f:
                                    config = json.load(f)
                                
                                # Display with custom title if available
                                title = config.get('title', config['name'])
                                description = config.get('description', '')
                                created_at = config['created_at']
                                
                                with st.expander(f"📊 {title}", expanded=False):
                                    st.write(f"**Name:** {config['name']}")
                                    if description:
                                        st.write(f"**Description:** {description}")
                                    st.write(f"**Created:** {created_at}")
                                    st.write(f"**Tables:** {len(config.get('tables', []))}")
                                    
                                    # Options to load or edit this dashboard
                                    col_load, col_edit = st.columns(2)
                                    with col_load:
                                        if st.button(f"🔄 Load", key=f"load_{config['name']}"):
                                            st.session_state['pipeline_components'] = config['pipeline_components']
                                            st.session_state['dashboard_title'] = title
                                            st.session_state['dashboard_description'] = description
                                            st.success(f"✅ Loaded dashboard: {title}")
                                            st.rerun()
                                    
                                    with col_edit:
                                        if st.button(f"✏️ Edit", key=f"edit_{config['name']}"):
                                            st.session_state['pipeline_components'] = config['pipeline_components']
                                            st.session_state['dashboard_title'] = title
                                            st.session_state['dashboard_description'] = description
                                            st.session_state['editing_dashboard'] = config['name']
                                            st.success(f"✅ Editing dashboard: {title}")
                                            st.rerun()
                        else:
                            st.info("No saved dashboards found")
                    else:
                        st.info("No saved dashboards directory found")
        
        # Show selected table information
        if 'selected_table' in st.session_state:
            selected_table = st.session_state['selected_table']
            st.subheader(f"📊 Selected Table: {selected_table['display_name']}")
            st.info(f"**Full Name:** {selected_table['full_name']}")
        
        # Original chart builder functionality
        if 'current_data' in st.session_state and 'selected_table' in st.session_state:
            df = st.session_state['current_data']
            analysis = st.session_state['schema_analysis']
            table_info = st.session_state['selected_table']
            
            # Table Info
            st.header(f"📊 Analyzing: {table_info['display_name']}")
            st.info(f"**Full Name:** {table_info['full_name']} | **Rows:** {len(df)} | **Columns:** {len(df.columns)}")
            
            # Data Preview
            with st.expander("📋 Data Preview", expanded=False):
                st.dataframe(df.head(20))
                st.write(f"**Data Types:**")
                st.json(df.dtypes.to_dict())
            
            # Schema Analysis
            with st.expander("🔍 Schema Analysis", expanded=True):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Numeric Columns", len(analysis['numeric_columns']))
                    if analysis['numeric_columns']:
                        st.write("**Numeric:**", ", ".join(analysis['numeric_columns'][:5]))
                        
                with col2:
                    st.metric("Categorical Columns", len(analysis['categorical_columns']))
                    if analysis['categorical_columns']:
                        st.write("**Categorical:**", ", ".join(analysis['categorical_columns'][:5]))
                        
                with col3:
                    st.metric("Datetime Columns", len(analysis['datetime_columns']))
                    if analysis['datetime_columns']:
                        st.write("**Datetime:**", ", ".join(analysis['datetime_columns'][:5]))
            
            # Architecture Diagram - Show pipeline architecture if available, otherwise single table
            pipeline_components = st.session_state.get('pipeline_components', [])
            
            if pipeline_components:
                st.header("🏗️ Pipeline Architecture")
                arch_fig = self.create_pipeline_architecture_diagram(pipeline_components)
                st.plotly_chart(arch_fig, use_container_width=True, key="pipeline_arch_diagram")
            else:
                st.header("🏗️ Data Architecture")
                arch_fig = self.create_architecture_diagram(table_info)
                st.plotly_chart(arch_fig, use_container_width=True, key="single_table_arch_diagram")
            
            # Chart Builder (Conditional)
            if st.session_state.get('enable_chart_builder', True):
                st.header("📈 Chart Builder")
                
                if analysis['suggested_charts']:
                    chart_type = st.selectbox(
                        "Select Chart Type",
                        options=analysis['suggested_charts'],
                        key="chart_type"
                    )
                    
                    # Column selection based on chart type
                    x_col = None
                    y_col = None
                    color_col = None
                    
                    if chart_type in ['histogram', 'bar_chart', 'pie_chart']:
                        x_col = st.selectbox("Select Column", options=df.columns, key="x_col")
                        
                    elif chart_type in ['scatter_plot', 'line_chart', 'box_plot']:
                        x_col = st.selectbox("X Column", options=df.columns, key="x_col")
                        y_col = st.selectbox("Y Column", options=analysis['numeric_columns'], key="y_col")
                        color_col = st.selectbox("Color Column (optional)", 
                                               options=['None'] + analysis['categorical_columns'], key="color_col")
                        if color_col == 'None':
                            color_col = None
                    
                    # Chart title
                    title = st.text_input("Chart Title (optional)", key="chart_title")
                    
                    # Create and display chart
                    if st.button("📊 Generate Chart"):
                        try:
                            fig = self.create_chart(df, chart_type, x_col, y_col, color_col, title)
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Save chart configuration
                            chart_config = {
                                'chart_type': chart_type,
                                'x_column': x_col,
                                'y_column': y_col,
                                'color_column': color_col,
                                'title': title,
                                'table': table_info['full_name'],
                                'created_at': datetime.now().isoformat()
                            }
                            
                            if 'saved_charts' not in st.session_state:
                                st.session_state['saved_charts'] = []
                            
                            st.session_state['saved_charts'].append(chart_config)
                            st.success("✅ Chart saved to dashboard!")
                            
                        except Exception as e:
                            st.error(f"Error creating chart: {e}")
            else:
                st.info("📈 Chart Builder is disabled. Enable it in the sidebar to build charts.")
            
            # Saved Charts
            if 'saved_charts' in st.session_state and st.session_state['saved_charts']:
                st.header("💾 Saved Charts")
                
                for i, chart_config in enumerate(st.session_state['saved_charts']):
                    with st.expander(f"📊 {chart_config['title'] or chart_config['chart_type']}", expanded=False):
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            # Recreate the chart
                            try:
                                # Get fresh data for the chart
                                fresh_df = self.get_table_data(
                                    chart_config['table'].split('.')[0],
                                    chart_config['table'].split('.')[1],
                                    chart_config['table'].split('.')[2],
                                    limit=1000
                                )
                                
                                fig = self.create_chart(
                                    fresh_df,
                                    chart_config['chart_type'],
                                    chart_config['x_column'],
                                    chart_config['y_column'],
                                    chart_config['color_column'],
                                    chart_config['title']
                                )
                                st.plotly_chart(fig, use_container_width=True, key=f"chart_{i}")
                                
                            except Exception as e:
                                st.error(f"Error recreating chart: {e}")
                        
                        with col2:
                            st.write(f"**Type:** {chart_config['chart_type']}")
                            st.write(f"**Table:** {chart_config['table']}")
                            st.write(f"**Created:** {chart_config['created_at'][:19]}")
                            
                            if st.button(f"🗑️ Delete", key=f"delete_{i}"):
                                st.session_state['saved_charts'].pop(i)
                                st.rerun()
        
        else:
            st.info("👆 Use the sidebar to discover tables and start building your dashboard!")
    
    def render_pipeline_builder(self):
        """Render the pipeline builder interface"""
        pipeline_components = st.session_state.get('pipeline_components', [])
        
        # Pipeline Architecture Diagram
        st.header("🏗️ Pipeline Architecture")
        arch_fig = self.create_pipeline_architecture_diagram(pipeline_components)
        st.plotly_chart(arch_fig, use_container_width=True, key="pipeline_builder_arch_diagram")
        
        # Show data for each component if loaded
        if pipeline_components and any(comp.get('data') for comp in pipeline_components):
            st.header("📊 Pipeline Data")
            
            for component in pipeline_components:
                if component.get('data') is not None:
                    df = component['data']
                    
                    with st.expander(f"📋 {component['layer_type']}: {component['table_name']}", expanded=False):
                        st.write(f"**Table:** {component['full_name']}")
                        if component.get('description'):
                            st.write(f"**Description:** {component['description']}")
                        if component.get('depends_on'):
                            st.write(f"**Depends On:** {component['depends_on']}")
                        
                        st.write(f"**Rows:** {len(df)} | **Columns:** {len(df.columns)}")
                        st.dataframe(df.head(10))
                        
                        # Schema analysis for this component
                        analysis = self.analyze_table_schema(df)
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Numeric", len(analysis['numeric_columns']))
                        with col2:
                            st.metric("Categorical", len(analysis['categorical_columns']))
                        with col3:
                            st.metric("Datetime", len(analysis['datetime_columns']))
    
    def render_dashboard_builder(self):
        """Render the dynamic dashboard builder interface"""
        st.title("🔧 Dynamic Dashboard Builder")
        st.markdown("Discover tables and build custom analytics on-the-fly!")
        
        # Sidebar for controls
        with st.sidebar:
            st.header("🎛️ Dashboard Controls")
            
            # Mode Selection
            st.subheader("🎯 Mode Selection")
            mode = st.radio(
                "Choose Mode:",
                ["Chart Builder", "Pipeline Builder"],
                key="mode_selector"
            )
            
            if mode == "Pipeline Builder":
                st.info("🏗️ Build a dynamic pipeline with custom components")
                
                # Pipeline Builder Controls
                st.subheader("🏗️ Pipeline Builder")
                
                # Initialize pipeline components if not exists
                if 'pipeline_components' not in st.session_state:
                    st.session_state['pipeline_components'] = []
                
                # Component Builder
                st.write("**🔧 Add Pipeline Component**")
                
                if 'discovered_tables' in st.session_state:
                    # Table selection
                    table_options = {t['display_name']: t for t in st.session_state['discovered_tables']}
                    selected_table_name = st.selectbox(
                        "Select Table:",
                        options=['None'] + list(table_options.keys()),
                        key="component_table_selector"
                    )
                    
                    if selected_table_name != 'None':
                        selected_table = table_options[selected_table_name]
                        
                        # Layer type selection
                        layer_type = st.selectbox(
                            "Layer Type:",
                            options=['Bronze', 'Silver', 'Gold', 'View', 'Custom'],
                            key="layer_type_selector"
                        )
                        
                        # Description
                        description = st.text_input(
                            "Description (optional):",
                            placeholder="e.g., Raw orders data, Transformed customer data, etc.",
                            key="component_description"
                        )
                        
                        # Dependency selection
                        existing_components = [comp.get('table_name') for comp in st.session_state['pipeline_components']]
                        dependency_options = ['None'] + existing_components
                        depends_on = st.selectbox(
                            "Depends On (optional):",
                            options=dependency_options,
                            key="dependency_selector"
                        )
                        
                        if depends_on == 'None':
                            depends_on = None
                        
                        # Add component button
                        if st.button("➕ Add Component"):
                            component = {
                                'table_name': selected_table['display_name'],
                                'full_name': selected_table['full_name'],
                                'layer_type': layer_type,
                                'description': description,
                                'depends_on': depends_on,
                                'catalog': selected_table['catalog'],
                                'schema': selected_table['schema'],
                                'table': selected_table['table']
                            }
                            
                            st.session_state['pipeline_components'].append(component)
                            st.success(f"✅ Added {layer_type} component: {selected_table['display_name']}")
                            st.rerun()
                
                # Pipeline Actions
                if st.session_state['pipeline_components']:
                    st.subheader("🚀 Pipeline Actions")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("📊 Load Pipeline Data"):
                            with st.spinner("Loading pipeline data..."):
                                for component in st.session_state['pipeline_components']:
                                    df = self.get_table_data(
                                        component['catalog'],
                                        component['schema'],
                                        component['table'],
                                        limit=1000
                                    )
                                    component['data'] = df
                                st.success("✅ Pipeline data loaded!")
                    
                    with col2:
                        if st.button("🗑️ Clear Pipeline"):
                            st.session_state['pipeline_components'] = []
                            st.rerun()
                    
                    # Show current components
                    st.write("**📋 Current Components:**")
                    for i, component in enumerate(st.session_state['pipeline_components']):
                        with st.expander(f"{component['layer_type']}: {component['table_name']}", expanded=False):
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                st.write(f"**Table:** {component['full_name']}")
                                if component['description']:
                                    st.write(f"**Description:** {component['description']}")
                                if component['depends_on']:
                                    st.write(f"**Depends On:** {component['depends_on']}")
                            
                            with col2:
                                if st.button(f"🗑️ Remove", key=f"remove_{i}"):
                                    st.session_state['pipeline_components'].pop(i)
                                    st.rerun()
            
            # Table Discovery (available for both modes)
            st.subheader("📋 Table Discovery")
            
            # Table discovery controls
            with st.expander("🔍 Configure Table Discovery", expanded=False):
                st.write("**Select where to search for tables:**")
                
                # Get available catalogs
                try:
                    from databricks.connect import DatabricksSession
                    spark = DatabricksSession.builder.remote().getOrCreate()
                    catalogs_df = spark.sql("SHOW CATALOGS")
                    available_catalogs = [row['catalog'] for row in catalogs_df.collect()]
                except Exception as e:
                    st.error(f"Error getting catalogs: {e}")
                    available_catalogs = ['timpomaville', 'hive_metastore', 'samples', 'system']
                
                # Catalog selection
                selected_catalogs = st.multiselect(
                    "Select Catalogs:",
                    options=available_catalogs,
                    default=['timpomaville'],
                    help="Choose which catalogs to search for tables"
                )
                
                # Schema selection (if specific catalog is selected)
                selected_schemas = []
                if len(selected_catalogs) == 1:
                    try:
                        schemas_df = spark.sql(f"SHOW SCHEMAS IN {selected_catalogs[0]}")
                        available_schemas = [row['namespace'] for row in schemas_df.collect()]
                        selected_schemas = st.multiselect(
                            "Select Schemas (optional):",
                            options=available_schemas,
                            default=['default'],
                            help="Leave empty to search all schemas"
                        )
                    except Exception as e:
                        st.warning(f"Could not get schemas for {selected_catalogs[0]}: {e}")
                        # Fallback to default schema
                        selected_schemas = ['default']
                
                # Search options
                col1, col2 = st.columns(2)
                with col1:
                    include_info_schema = st.checkbox(
                        "Include information_schema tables",
                        value=False,
                        help="Include system tables (usually not needed)"
                    )
                
                with col2:
                    max_tables = st.number_input(
                        "Max tables to discover:",
                        min_value=10,
                        max_value=1000,
                        value=100,
                        help="Limit the number of tables to discover"
                    )
                
                # Discover button
                if st.button("🔍 Discover Tables", type="primary"):
                    if not selected_catalogs:
                        st.error("Please select at least one catalog!")
                    else:
                        with st.spinner(f"Discovering tables in {', '.join(selected_catalogs)}..."):
                            tables = self.discover_tables_controlled(
                                selected_catalogs, 
                                selected_schemas, 
                                include_info_schema, 
                                max_tables
                            )
                            st.session_state['discovered_tables'] = tables
                            st.success(f"✅ Discovered {len(tables)} tables!")
                            st.rerun()
                
                # Table Selection (only for Chart Builder mode)
                if 'discovered_tables' in st.session_state and st.session_state['discovered_tables']:
                    st.subheader("📋 Table Selection")
                    
                    # Show discovered tables count
                    st.info(f"📊 {len(st.session_state['discovered_tables'])} tables discovered")
                    
                    # Table selection dropdown
                    table_options = {t['display_name']: t for t in st.session_state['discovered_tables']}
                    selected_table_name = st.selectbox(
                        "Select Table:",
                        options=['None'] + list(table_options.keys()),
                        key="table_selector"
                    )
                    
                    if selected_table_name and selected_table_name != 'None':
                        selected_table = table_options[selected_table_name]
                        st.session_state['selected_table'] = selected_table
                        
                        # Show selected table info
                        st.success(f"✅ Selected: {selected_table['full_name']}")
                        
                        # Data loading controls
                        st.subheader("📊 Data Controls")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            limit = st.slider("Row Limit", 100, 10000, 1000, key="data_limit_slider")
                        
                        with col2:
                            # Date range filters
                            st.write("**Date Range (if applicable):**")
                            use_date_filter = st.checkbox("Use Date Filter", value=False, key="use_date_filter")
                            
                            if use_date_filter:
                                col_date1, col_date2 = st.columns(2)
                                with col_date1:
                                    start_date = st.date_input("Start Date", value=datetime.now().date() - timedelta(days=30), key="start_date")
                                with col_date2:
                                    end_date = st.date_input("End Date", value=datetime.now().date(), key="end_date")
                            else:
                                start_date = None
                                end_date = None
                        
                        if st.button("📥 Load Data", key="load_data_btn"):
                            with st.spinner("Loading data..."):
                                # Build query with date filter if enabled
                                if use_date_filter and start_date and end_date:
                                    # Try to find date columns for filtering
                                    df = self.get_table_data_with_date_filter(
                                        selected_table['catalog'],
                                        selected_table['schema'], 
                                        selected_table['table'],
                                        limit,
                                        start_date,
                                        end_date
                                    )
                                else:
                                    df = self.get_table_data(
                                        selected_table['catalog'],
                                        selected_table['schema'], 
                                        selected_table['table'],
                                        limit
                                    )
                                st.session_state['current_data'] = df
                                st.session_state['schema_analysis'] = self.analyze_table_schema(df)
                                st.success(f"✅ Loaded {len(df)} rows!")
                                st.rerun()
                        
                        # Show data preview if loaded
                        if 'current_data' in st.session_state and st.session_state['current_data'] is not None:
                            df = st.session_state['current_data']
                            st.subheader("📋 Data Preview")
                            st.write(f"**Rows:** {len(df)} | **Columns:** {len(df.columns)}")
                            st.dataframe(df.head(5))
                else:
                    st.info("🔍 Click 'Discover Tables' to find available tables")
        
        # Main content area
        mode = st.session_state.get('mode_selector', 'Single Table Explorer')
        
        if mode == "Pipeline Builder":
            self.render_pipeline_builder()
        else:
            self.render_chart_builder()

def main():
    """Main function to run the dynamic dashboard"""
    st.set_page_config(
        page_title="Dynamic Dashboard Builder",
        page_icon="🔧",
        layout="wide"
    )
    
    dashboard = DynamicDashboard()
    dashboard.render_dashboard_builder()

if __name__ == "__main__":
    main()
