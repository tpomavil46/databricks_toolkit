#!/usr/bin/env python3
"""
Dashboard Launcher

This script launches the Streamlit dashboard for the retail analytics pipeline.
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Launch the Streamlit dashboard."""
    
    # Get the dashboard directory
    dashboard_dir = Path(__file__).parent / "dashboard"
    
    if not dashboard_dir.exists():
        print("❌ Dashboard directory not found!")
        print(f"Expected path: {dashboard_dir}")
        sys.exit(1)
    
    # Check if requirements are installed
    try:
        import streamlit
        import plotly
        import pandas
        import numpy
        print("✅ All dashboard dependencies are installed!")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Installing dashboard requirements...")
        
        # Install requirements
        requirements_file = dashboard_dir / "requirements.txt"
        if requirements_file.exists():
            subprocess.run([
                sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
            ], check=True)
            print("✅ Dashboard dependencies installed!")
        else:
            print("❌ Requirements file not found!")
            sys.exit(1)
    
    # Launch the dashboard
    print("🚀 Launching Retail Analytics Dashboard...")
    print("📊 Dashboard will open in your browser at http://localhost:8501")
    print("🔄 Press Ctrl+C to stop the dashboard")
    
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(dashboard_dir / "app.py"),
            "--server.port", "8501",
            "--server.address", "localhost"
        ])
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped!")
    except Exception as e:
        print(f"❌ Error launching dashboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 