"""
Quick script to collect copper spread data
Version: 1.0
Date: 2025-07-18

Simple script to collect LME copper spread data into JCL database.
"""

import os
import sys
from datetime import datetime
from sql_data_collector_jcl import SQLServerDataCollectorJCL


def main():
    print("\n" + "="*60)
    print("LME COPPER SPREAD DATA COLLECTION")
    print("="*60)
    print(f"Started at: {datetime.now()}")
    
    # Check config file in root directory
    config_file = os.path.join(os.path.dirname(__file__), '..', '..', 'config.jcl.json')
    config_file = os.path.abspath(config_file)
    
    if not os.path.exists(config_file):
        print(f"\nERROR: config.jcl.json not found in {os.path.dirname(config_file)}")
        print("Please create it from config.example.jcl.json")
        return 1
    
    print(f"Using config file: {config_file}")
    
    # Create collector
    collector = SQLServerDataCollectorJCL(config_file)
    
    try:
        # 1. Connect to database
        print("\n1. Connecting to JCL database...")
        if not collector.connect_database():
            print("ERROR: Failed to connect to database")
            return 1
        print("✓ Connected to database")
        
        # 2. Connect to Bloomberg
        print("\n2. Connecting to Bloomberg...")
        if not collector.start_bloomberg_session():
            print("ERROR: Failed to connect to Bloomberg")
            print("Make sure Bloomberg Terminal is running")
            return 1
        print("✓ Connected to Bloomberg")
        
        # 3. Search for copper spreads
        print("\n3. Searching for copper spreads...")
        spreads = collector.search_spreads('CU')
        print(f"✓ Found {len(spreads)} copper spreads")
        
        # Show spread type distribution
        spread_types = {}
        for spread in spreads:
            st = spread['spread_type']
            spread_types[st] = spread_types.get(st, 0) + 1
        
        print("\nSpread types found:")
        for spread_type, count in sorted(spread_types.items()):
            print(f"  {spread_type}: {count}")
        
        # 4. Store spread definitions
        print("\n4. Storing spread definitions...")
        stored = collector.store_spreads(spreads)
        print(f"✓ Stored {stored} spread definitions")
        
        # 5. Get market data (in batches)
        print("\n5. Getting market data...")
        batch_size = 100
        total_market_data = []
        
        for i in range(0, len(spreads), batch_size):
            batch = spreads[i:i+batch_size]
            print(f"  Processing batch {i//batch_size + 1}/{(len(spreads) + batch_size - 1)//batch_size}...")
            
            market_data = collector.get_market_data(batch)
            total_market_data.extend(market_data)
            
            # Store this batch
            if market_data:
                stored = collector.store_tick_data(market_data)
                print(f"    Stored {stored} tick records")
        
        print(f"\n✓ Total market data collected: {len(total_market_data)} records")
        
        # 6. Show summary
        print("\n6. Data Summary:")
        cursor = collector.connection.cursor()
        
        # Total spreads
        cursor.execute("SELECT COUNT(*) FROM lme_market.LME_M_spreads WHERE metal_id = 1")
        total_spreads = cursor.fetchone()[0]
        print(f"  Total copper spreads in database: {total_spreads}")
        
        # Today's ticks
        cursor.execute("""
            SELECT COUNT(*) FROM lme_market.LME_T_tick_data t
            JOIN lme_market.LME_M_spreads s ON t.spread_id = s.spread_id
            WHERE s.metal_id = 1 
            AND CAST(t.timestamp AS DATE) = CAST(GETDATE() AS DATE)
        """)
        today_ticks = cursor.fetchone()[0]
        print(f"  Tick records collected today: {today_ticks}")
        
        # Active spreads
        cursor.execute("""
            SELECT COUNT(DISTINCT ticker) 
            FROM lme_market.V_latest_market_data 
            WHERE metal_code = 'CU' 
            AND data_freshness = 'Active'
        """)
        active_spreads = cursor.fetchone()[0]
        print(f"  Currently active spreads: {active_spreads}")
        
        # Today's volume
        cursor.execute("""
            SELECT COUNT(*), SUM(volume) 
            FROM lme_market.V_todays_activity 
            WHERE metal_code = 'CU'
        """)
        result = cursor.fetchone()
        if result[0] > 0:
            print(f"  Spreads with volume today: {result[0]}")
            print(f"  Total volume today: {result[1] or 0}")
        
        cursor.close()
        
        # 7. Update collection status
        print("\n7. Updating collection status...")
        collector.update_collection_status('CU', 'REALTIME')
        print("✓ Collection status updated")
        
        print("\n" + "="*60)
        print("DATA COLLECTION COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        print("\nTo view the data in SSMS, run:")
        print("  SELECT TOP 100 * FROM lme_market.V_latest_market_data WHERE metal_code = 'CU'")
        print("  SELECT * FROM lme_market.V_todays_activity WHERE metal_code = 'CU'")
        
        return 0
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        collector.close()
        print(f"\nFinished at: {datetime.now()}")


if __name__ == "__main__":
    sys.exit(main())