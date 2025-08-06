#!/usr/bin/env python3
"""
Unified Dashboard Launcher
Choose between different dashboard types:
1. Business Analytics Dashboard
2. GCP Data Engineering Costs Dashboard  
3. Dynamic Dashboard Builder
"""

import os
import sys
import subprocess
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    package_checks = [
        ('streamlit', 'streamlit'),
        ('pandas', 'pandas'),
        ('plotly', 'plotly'),
        ('databricks-connect', 'databricks.connect')
    ]
    
    missing_packages = []
    for package_name, import_name in package_checks:
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package_name)
    
    if missing_packages:
        print("❌ Missing required packages:")
        for package in missing_packages:
            print(f"   - {package}")
        print("\n📦 Install missing packages with:")
        print(f"   pip install {' '.join(missing_packages)}")
        return False
    
    print("✅ All dashboard dependencies are installed!")
    return True

def launch_dashboard(dashboard_type: str, port: int = 8501):
    """Launch a specific dashboard type"""
    # Since this launcher is in the dashboard folder, look in current directory
    dashboard_dir = Path(__file__).parent
    
    if dashboard_type == "business":
        dashboard_file = dashboard_dir / "app.py"
        title = "Business Analytics Dashboard"
    elif dashboard_type == "gcp":
        dashboard_file = dashboard_dir / "real_gcp_view.py"
        title = "GCP Data Engineering Costs Dashboard"
    elif dashboard_type == "dynamic":
        dashboard_file = dashboard_dir / "dynamic_dashboard.py"
        title = "Dynamic Dashboard Builder"
        port = 8502
    else:
        print(f"❌ Unknown dashboard type: {dashboard_type}")
        return False
    
    if not dashboard_file.exists():
        print(f"❌ Dashboard file not found: {dashboard_file}")
        return False
    
    print(f"🚀 Launching {title}...")
    print(f"📊 Dashboard will open in your browser at http://localhost:{port}")
    print("🔄 Press Ctrl+C to stop the dashboard")
    print("=" * 50)
    
    try:
        subprocess.run([
            "streamlit", "run", str(dashboard_file),
            "--server.port", str(port),
            "--server.headless", "true"
        ])
        return True
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped by user")
        return True
    except Exception as e:
        print(f"❌ Error launching dashboard: {e}")
        return False

def main():
    """Main function to show dashboard options"""
    print("🎯 Databricks Toolkit Dashboard Launcher")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Set up environment
    os.environ['GCP_PROJECT_ID'] = "mydatabrickssandbox"
    
    while True:
        print("\n📊 Choose a Dashboard:")
        print("1. 🏪 Business Analytics Dashboard")
        print("   - Retail analytics with real Databricks data")
        print("   - Business performance metrics")
        print("   - Revenue and product analysis")
        print()
        print("2. ☁️ GCP Data Engineering Costs Dashboard")
        print("   - Real GCP billing data ($2.41 total)")
        print("   - BigQuery, Cloud Storage, Compute costs")
        print("   - Data engineering cost monitoring")
        print()
        print("3. 🔧 Dynamic Dashboard Builder")
        print("   - Discover tables in your Databricks workspace")
        print("   - Build custom analytics on-the-fly")
        print("   - Create and save custom charts")
        print()
        print("4. 🚪 Exit")
        print()
        
        choice = input("Enter your choice (1-4): ").strip()
        
        if choice == "1":
            launch_dashboard("business", 8501)
        elif choice == "2":
            launch_dashboard("gcp", 8501)
        elif choice == "3":
            launch_dashboard("dynamic", 8502)
        elif choice == "4":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, 3, or 4.")

if __name__ == "__main__":
    main()
