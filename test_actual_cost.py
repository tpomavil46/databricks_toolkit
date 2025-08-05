#!/usr/bin/env python3
"""
Test Actual GCP Cost
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_actual_cost():
    """Test that we show the actual $2.41 cost."""
    
    # Set the project ID
    os.environ['GCP_PROJECT_ID'] = 'mydatabrickssandbox'
    
    try:
        from dashboard.cloud_integrations import CostMonitor
        
        print("🔍 TESTING ACTUAL GCP COST")
        print("=" * 50)
        
        # Test cost monitoring
        cost_monitor = CostMonitor()
        cost_breakdown = cost_monitor.get_total_cost_breakdown()
        
        print(f"✅ Cost breakdown shape: {cost_breakdown.shape}")
        
        if not cost_breakdown.empty:
            gcp_costs = cost_breakdown[cost_breakdown['provider'] == 'gcp']
            if not gcp_costs.empty:
                print(f"🎯 GCP costs found: {len(gcp_costs)}")
                
                for _, cost in gcp_costs.iterrows():
                    service = cost['service']
                    amount = cost['cost']
                    is_real = cost.get('is_real_data', False)
                    print(f"💰 {service}: ${amount:.2f} (Real: {is_real})")
                
                # Get the total cost (not sum of individual services)
                total_cost_row = gcp_costs[gcp_costs['service'] == 'total']
                total_cost = total_cost_row['cost'].sum() if not total_cost_row.empty else gcp_costs['cost'].sum()
                print(f"\n💳 Total GCP Cost: ${total_cost:.2f}")
                
                if abs(total_cost - 2.41) < 0.01:
                    print("✅ SUCCESS! Dashboard shows your actual $2.41 cost!")
                else:
                    print(f"❌ Expected $2.41, got ${total_cost:.2f}")
            else:
                print("❌ No GCP costs found")
        else:
            print("❌ No cost breakdown found")
            
    except Exception as e:
        print(f"❌ Error testing actual cost: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_actual_cost() 