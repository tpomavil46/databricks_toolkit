#!/usr/bin/env python3
"""
Dynamic Dashboard Launcher
Launches the dynamic dashboard builder for table discovery and custom analytics
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

def main():
    """Main function to launch the dynamic dashboard"""
    print("🔧 Dynamic Dashboard Builder")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Set up environment
    os.environ['GCP_PROJECT_ID'] = "mydatabrickssandbox"
    
    # Get the dashboard directory
    dashboard_dir = Path(__file__).parent / "dashboard"
    dynamic_dashboard_file = dashboard_dir / "dynamic_dashboard.py"
    
    if not dynamic_dashboard_file.exists():
        print(f"❌ Dynamic dashboard file not found: {dynamic_dashboard_file}")
        sys.exit(1)
    
    print("🚀 Launching Dynamic Dashboard Builder...")
    print("📊 Dashboard will open in your browser at http://localhost:8502")
    print("🔄 Press Ctrl+C to stop the dashboard")
    print("=" * 50)
    
    try:
        # Launch the dynamic dashboard
        subprocess.run([
            "streamlit", "run", str(dynamic_dashboard_file),
            "--server.port", "8502",
            "--server.headless", "true"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Error launching dashboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
