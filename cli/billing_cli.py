#!/usr/bin/env python3
"""
Billing CLI for Databricks toolkit.

Usage:
    python cli/billing_cli.py costs --year 2025 --month 8
    python cli/billing_cli.py report --year 2025 --month 8
    python cli/billing_cli.py check --threshold 100
    python cli/billing_cli.py dashboard
"""

import argparse
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.billing_monitor import BillingMonitor, BillingCLI, BillingDashboard


def main():
    parser = argparse.ArgumentParser(description="Databricks Billing Monitor CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Costs command
    costs_parser = subparsers.add_parser("costs", help="Get monthly costs")
    costs_parser.add_argument("--year", type=int, default=datetime.now().year, help="Year (default: current)")
    costs_parser.add_argument("--month", type=int, default=datetime.now().month, help="Month (1-12, default: current)")
    costs_parser.add_argument("--output", help="Output CSV file")
    
    # Report command
    report_parser = subparsers.add_parser("report", help="Generate comprehensive cost report")
    report_parser.add_argument("--year", type=int, default=datetime.now().year, help="Year (default: current)")
    report_parser.add_argument("--month", type=int, default=datetime.now().month, help="Month (1-12, default: current)")
    
    # Check command
    check_parser = subparsers.add_parser("check", help="Check cost threshold")
    check_parser.add_argument("--threshold", type=float, default=100.0, help="Cost threshold in dollars")
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser("dashboard", help="Get dashboard data")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        monitor = BillingMonitor()
        cli = BillingCLI(monitor)
        
        if args.command == "costs":
            cli.run_monthly_report(args.year, args.month, args.output)
        
        elif args.command == "report":
            import json
            report = monitor.generate_cost_report(args.year, args.month)
            print(json.dumps(report, indent=2, default=str))
        
        elif args.command == "check":
            cli.run_cost_check(args.threshold)
        
        elif args.command == "dashboard":
            import json
            dashboard = BillingDashboard(monitor)
            data = dashboard.get_dashboard_data()
            print(json.dumps(data, indent=2, default=str))
    
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
