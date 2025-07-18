"""
Test script for JCL Database LME System
Version: 1.0
Date: 2025-07-18

This script tests the LME data collection system in JCL database.
"""

import pyodbc
from datetime import datetime, date, timedelta
from sql_data_collector_jcl import SQLServerDataCollectorJCL
import pandas as pd
import os


class JCLSystemTester:
    """Test LME system in JCL database"""
    
    def __init__(self, config_path: str = None):
        # Default to root directory config file
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.jcl.json')
            config_path = os.path.abspath(config_path)
            
        # Check if config file exists
        if not os.path.exists(config_path):
            print(f"ERROR: {config_path} not found!")
            print("Please create config.jcl.json from config.example.jcl.json")
            print(f"Looking in: {os.path.dirname(config_path)}")
            raise FileNotFoundError(config_path)
            
        print(f"Using config file: {config_path}")
        self.collector = SQLServerDataCollectorJCL(config_path)
        
    def test_database_connection(self):
        """Test database connectivity"""
        print("="*60)
        print("Testing JCL Database Connection")
        print("="*60)
        
        if self.collector.connect_database():
            print("✓ JCL database connection successful")
            
            cursor = self.collector.connection.cursor()
            
            # Check current database
            cursor.execute("SELECT DB_NAME()")
            db_name = cursor.fetchone()[0]
            print(f"✓ Connected to database: {db_name}")
            
            # Check LME schemas exist
            cursor.execute("""
                SELECT COUNT(*) FROM sys.schemas 
                WHERE name IN ('lme_market', 'lme_config')
            """)
            schema_count = cursor.fetchone()[0]
            print(f"✓ LME schemas found: {schema_count}/2")
            
            cursor.close()
        else:
            print("✗ Database connection failed")
            return False
            
        return True
        
    def test_initial_data(self):
        """Test initial master data"""
        print("\n" + "="*60)
        print("Testing Initial Master Data")
        print("="*60)
        
        cursor = self.collector.connection.cursor()
        
        try:
            # Check metals
            cursor.execute("SELECT COUNT(*) FROM lme_config.LME_M_metals")
            metal_count = cursor.fetchone()[0]
            print(f"✓ Metals configured: {metal_count}")
            
            cursor.execute("SELECT metal_code, metal_name FROM lme_config.LME_M_metals ORDER BY metal_id")
            print("\nConfigured metals:")
            for row in cursor.fetchall():
                print(f"  - {row[0]}: {row[1]}")
                
            # Check collection config
            cursor.execute("SELECT COUNT(*) FROM lme_config.LME_M_collection_config")
            config_count = cursor.fetchone()[0]
            print(f"\n✓ Collection configurations: {config_count}")
            
        finally:
            cursor.close()
            
    def test_bloomberg_connection(self):
        """Test Bloomberg API connectivity"""
        print("\n" + "="*60)
        print("Testing Bloomberg Connection")
        print("="*60)
        
        if self.collector.start_bloomberg_session():
            print("✓ Bloomberg session started successfully")
            return True
        else:
            print("✗ Bloomberg session failed")
            print("  Make sure Bloomberg Terminal is running")
            return False
            
    def test_spread_search(self, metal_code: str = 'CU'):
        """Test spread search functionality"""
        print("\n" + "="*60)
        print(f"Testing Spread Search for {metal_code}")
        print("="*60)
        
        spreads = self.collector.search_spreads(metal_code)
        print(f"✓ Found {len(spreads)} spreads")
        
        # Show sample spreads by type
        spread_types = {}
        for spread in spreads:
            spread_type = spread['spread_type']
            if spread_type not in spread_types:
                spread_types[spread_type] = []
            spread_types[spread_type].append(spread['ticker'])
            
        print("\nSample spreads by type:")
        for spread_type, tickers in spread_types.items():
            print(f"  {spread_type}: {len(tickers)} spreads")
            if tickers:
                print(f"    Example: {tickers[0]}")
            
        return spreads
        
    def test_data_storage(self, spreads):
        """Test storing spread definitions"""
        print("\n" + "="*60)
        print("Testing Data Storage")
        print("="*60)
        
        # Check existing spreads
        cursor = self.collector.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM lme_market.LME_M_spreads")
        before_count = cursor.fetchone()[0]
        cursor.close()
        
        # Store first 20 spreads as test
        test_spreads = spreads[:20]
        stored = self.collector.store_spreads(test_spreads)
        print(f"✓ Processed {stored} spread definitions")
        
        # Check after storage
        cursor = self.collector.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM lme_market.LME_M_spreads")
        after_count = cursor.fetchone()[0]
        cursor.close()
        
        print(f"  Spreads before: {before_count}")
        print(f"  Spreads after: {after_count}")
        print(f"  New spreads: {after_count - before_count}")
        
        return test_spreads
        
    def test_market_data_collection(self, spreads):
        """Test market data collection"""
        print("\n" + "="*60)
        print("Testing Market Data Collection")
        print("="*60)
        
        # Get market data for test spreads (first 5)
        test_spreads = spreads[:5]
        print(f"Getting market data for {len(test_spreads)} spreads...")
        
        market_data = self.collector.get_market_data(test_spreads)
        print(f"✓ Collected market data for {len(market_data)} spreads")
        
        # Show sample data
        if market_data:
            sample = market_data[0]
            print("\nSample market data:")
            print(f"  Ticker: {sample['ticker']}")
            print(f"  Bid: {sample.get('BID', 'N/A')}")
            print(f"  Ask: {sample.get('ASK', 'N/A')}")
            print(f"  Last: {sample.get('LAST_PRICE', 'N/A')}")
            print(f"  Volume: {sample.get('VOLUME', 'N/A')}")
            
        return market_data
        
    def test_tick_storage(self, market_data):
        """Test storing tick data"""
        print("\n" + "="*60)
        print("Testing Tick Data Storage")
        print("="*60)
        
        if not market_data:
            print("No market data to store")
            return
            
        stored = self.collector.store_tick_data(market_data)
        print(f"✓ Stored {stored} tick records")
        
        # Verify stored data
        cursor = self.collector.connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM lme_market.LME_T_tick_data
            WHERE CAST(timestamp AS DATE) = CAST(GETDATE() AS DATE)
        """)
        today_count = cursor.fetchone()[0]
        print(f"  Total ticks today: {today_count}")
        cursor.close()
        
    def test_views(self):
        """Test database views"""
        print("\n" + "="*60)
        print("Testing Database Views")
        print("="*60)
        
        cursor = self.collector.connection.cursor()
        
        try:
            # Test latest market data view
            cursor.execute("""
                SELECT TOP 5 
                    metal_code, ticker, bid, ask, last_price, 
                    todays_volume, data_freshness
                FROM lme_market.V_latest_market_data
                ORDER BY todays_volume DESC
            """)
            
            print("\n✓ Latest Market Data (Top 5 by volume):")
            print(f"{'Metal':6} {'Ticker':30} {'Bid':10} {'Ask':10} {'Last':10} {'Volume':10} {'Status':10}")
            print("-"*96)
            
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(f"{row[0]:6} {row[1]:30} {str(row[2] or '---'):10} {str(row[3] or '---'):10} "
                          f"{str(row[4] or '---'):10} {row[5] or 0:10} {row[6]:10}")
            else:
                print("  No data found yet")
                      
            # Test collection health view
            cursor.execute("""
                SELECT metal_code, collection_type, collection_status
                FROM lme_config.V_collection_health
                WHERE metal_code = 'CU'
            """)
            
            print("\n✓ Collection Health (Copper):")
            for row in cursor.fetchall():
                print(f"  {row[0]} - {row[1]}: {row[2]}")
                
        finally:
            cursor.close()
            
    def run_quick_test(self):
        """Run a quick test to verify system setup"""
        print("\nLME System Quick Test for JCL Database")
        print("="*60)
        print(f"Test started at: {datetime.now()}")
        
        try:
            # Test connections
            if not self.test_database_connection():
                return
                
            self.test_initial_data()
                
            if not self.test_bloomberg_connection():
                print("\nBloomberg connection required for full test")
                print("Partial test completed - database setup verified")
                return
                
            # Quick data collection test
            print("\n" + "="*60)
            print("Running Quick Data Collection Test")
            print("="*60)
            
            # Search for just a few copper spreads
            spreads = self.collector.search_spreads('CU')[:10]
            print(f"Testing with {len(spreads)} copper spreads")
            
            # Store spreads
            stored = self.collector.store_spreads(spreads)
            print(f"✓ Stored {stored} spreads")
            
            # Get and store market data
            market_data = self.collector.get_market_data(spreads[:3])
            if market_data:
                tick_count = self.collector.store_tick_data(market_data)
                print(f"✓ Stored {tick_count} tick records")
            
            # Test views
            self.test_views()
            
            print("\n" + "="*60)
            print("Quick test completed successfully!")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ Test failed with error: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            self.collector.close()
            

def main():
    """Run system tests"""
    print("\n" + "="*60)
    print("LME SYSTEM TEST FOR JCL DATABASE")
    print("="*60)
    
    # Check for config file in root directory
    root_config = os.path.join(os.path.dirname(__file__), '..', '..', 'config.jcl.json')
    root_config = os.path.abspath(root_config)
    
    if not os.path.exists(root_config):
        print(f"\nERROR: config.jcl.json not found in {os.path.dirname(root_config)}")
        print("\nPlease create it by copying config.example.jcl.json:")
        print("  copy config.example.jcl.json config.jcl.json")
        print("\nThen edit it with your database connection details.")
        return
    
    tester = JCLSystemTester()
    tester.run_quick_test()
    

if __name__ == "__main__":
    main()