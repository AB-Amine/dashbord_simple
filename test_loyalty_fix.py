#!/usr/bin/env python3
"""
Test script to verify and fix the loyalty tier calculation logic.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

def test_current_loyalty_logic():
    """Test the current loyalty tier logic with the existing configuration."""
    print("🔍 Testing current loyalty tier logic...")
    
    config = Config()
    print(f"Current thresholds:")
    print(f"  Bronze (PROMO): > €{config.REVENUE_THRESHOLD_PROMO}")
    print(f"  Silver: > €{config.REVENUE_THRESHOLD_SILVER}")
    print(f"  Gold: > €{config.REVENUE_THRESHOLD_GOLD}")
    
    # Test different revenue amounts
    test_cases = [
        (500, 'Standard'),
        (5000, 'Standard'),  # Should be Bronze but will be Standard due to threshold issue
        (15000, 'Silver'),
        (25000, 'Gold'),
    ]
    
    print(f"\n📊 Testing revenue amounts:")
    for revenue, expected in test_cases:
        # Simulate the logic
        new_tier = 'Standard'
        if revenue > config.REVENUE_THRESHOLD_GOLD:
            new_tier = 'Gold'
        elif revenue > config.REVENUE_THRESHOLD_SILVER:
            new_tier = 'Silver'
        elif revenue > config.REVENUE_THRESHOLD_PROMO:
            new_tier = 'Bronze'
        
        status = "✅" if new_tier == expected else "❌"
        print(f"  €{revenue:8d} → {new_tier:8s} (expected: {expected:8s}) {status}")

def test_fixed_loyalty_logic():
    """Test the loyalty tier logic with corrected thresholds."""
    print("\n🔧 Testing with corrected thresholds...")
    
    # Proposed corrected thresholds
    corrected_thresholds = {
        'bronze': 1000,
        'silver': 10000,
        'gold': 20000
    }
    
    print(f"Corrected thresholds:")
    print(f"  Bronze: > €{corrected_thresholds['bronze']}")
    print(f"  Silver: > €{corrected_thresholds['silver']}")
    print(f"  Gold: > €{corrected_thresholds['gold']}")
    
    # Test different revenue amounts
    test_cases = [
        (500, 'Standard'),
        (1500, 'Bronze'),
        (5000, 'Bronze'),
        (15000, 'Silver'),
        (25000, 'Gold'),
    ]
    
    print(f"\n📊 Testing revenue amounts with corrected logic:")
    for revenue, expected in test_cases:
        # Simulate the corrected logic
        new_tier = 'Standard'
        if revenue > corrected_thresholds['gold']:
            new_tier = 'Gold'
        elif revenue > corrected_thresholds['silver']:
            new_tier = 'Silver'
        elif revenue > corrected_thresholds['bronze']:
            new_tier = 'Bronze'
        
        status = "✅" if new_tier == expected else "❌"
        print(f"  €{revenue:8d} → {new_tier:8s} (expected: {expected:8s}) {status}")

def main():
    """Run the loyalty tier tests."""
    print("🚀 Loyalty Tier Calculation Analysis")
    print("=" * 50)
    
    test_current_loyalty_logic()
    test_fixed_loyalty_logic()
    
    print(f"\n📋 RECOMMENDATION:")
    print(f"The current configuration has REVENUE_THRESHOLD_SILVER and REVENUE_THRESHOLD_PROMO")
    print(f"both set to 10000, which means Bronze tier can never be reached.")
    print(f"")
    print(f"Suggested fix:")
    print(f"1. Change REVENUE_THRESHOLD_PROMO to 1000 (for Bronze tier)")
    print(f"2. Keep REVENUE_THRESHOLD_SILVER at 10000 (for Silver tier)")
    print(f"3. Keep REVENUE_THRESHOLD_GOLD at 20000 (for Gold tier)")
    print(f"")
    print(f"This will create proper tier progression:")
    print(f"  Standard: €0 - €1000")
    print(f"  Bronze: €1000 - €10000")
    print(f"  Silver: €10000 - €20000")
    print(f"  Gold: €20000+")

if __name__ == "__main__":
    main()