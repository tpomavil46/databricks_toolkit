#!/usr/bin/env python3
"""
Launcher for Real GCP Data View
"""

import subprocess
import sys
import os

def main():
    """Launch the clean GCP data view."""
    
    print("🚀 Launching Real GCP Data View...")
    print("=" * 50)
    
    # Set up environment
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    
    # Launch the clean view
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "dashboard/real_gcp_view.py",
            "--server.port", "8502",
            "--server.headless", "true"
        ])
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped.")
    except Exception as e:
        print(f"❌ Error launching dashboard: {e}")

if __name__ == "__main__":
    main() 