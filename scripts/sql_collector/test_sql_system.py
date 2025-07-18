"""
Test script for SQL Server system
Version: 1.0
Date: 2025-07-18

This script tests the SQL Server data collection and analysis system.
"""

import pyodbc
from datetime import datetime, date, timedelta
from sql_data_collector import SQLServerDataCollector
import pandas as pd


class SQLSystemTester:
    """Test SQL Server spread collection system"""
    
    def __init__(self, config_path: str = "config.json"):
        self.collector = SQLServerDataCollector(config_path)
        
    def test_database_connection(self):
        """Test database connectivity"""
        print("="*60)
        print("Testing Database Connection")
        print("="*60)
        
        if self.collector.connect_database():
            print("✓ Database connection successful")
            
            cursor = self.collector.connection.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            print(f"✓ SQL Server Version: {version.split('\\n')[0]}")
            cursor.close()
        else:
            print("✗ Database connection failed")
            return False
            
        return True
        
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
            print(f"    Example: {tickers[0]}")
            
        return spreads
        
    def test_data_storage(self, spreads):
        """Test storing spread definitions"""
        print("\n" + "="*60)
        print("Testing Data Storage")
        print("="*60)
        
        # Store first 10 spreads as test
        test_spreads = spreads[:10]
        stored = self.collector.store_spreads(test_spreads)
        print(f"✓ Stored {stored} spread definitions")
        
        return test_spreads
        
    def test_market_data_collection(self, spreads):
        """Test market data collection"""
        print("\n" + "="*60)
        print("Testing Market Data Collection")
        print("="*60)
        
        # Get market data for test spreads
        market_data = self.collector.get_market_data(spreads[:5])
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
        
        stored = self.collector.store_tick_data(market_data)
        print(f"✓ Stored {stored} tick records")
        
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
                FROM market.v_latest_market_data
                ORDER BY todays_volume DESC
            """)
            
            print("\n✓ Latest Market Data (Top 5 by volume):")
            print(f"{'Metal':6} {'Ticker':30} {'Bid':10} {'Ask':10} {'Last':10} {'Volume':10} {'Status':10}")
            print("-"*96)
            
            for row in cursor.fetchall():
                print(f"{row[0]:6} {row[1]:30} {row[2] or '---':10} {row[3] or '---':10} "
                      f"{row[4] or '---':10} {row[5] or 0:10} {row[6]:10}")
                      
            # Test today's activity view
            cursor.execute("""
                SELECT COUNT(*) FROM market.v_todays_activity
            """)
            
            count = cursor.fetchone()[0]
            print(f"\n✓ Today's Activity: {count} spreads with volume")
            
            # Test spread type summary
            cursor.execute("""
                SELECT metal_code, spread_type, spread_count, active_count
                FROM market.v_spread_type_summary
                WHERE metal_code = 'CU'
                ORDER BY spread_count DESC
            """)
            
            print("\n✓ Spread Type Summary (Copper):")
            print(f"{'Type':15} {'Total':10} {'Active':10}")
            print("-"*35)
            
            for row in cursor.fetchall():
                print(f"{row[1]:15} {row[2]:10} {row[3]:10}")
                
        finally:
            cursor.close()
            
    def test_analysis_procedures(self):
        """Test analysis stored procedures"""
        print("\n" + "="*60)
        print("Testing Analysis Procedures")
        print("="*60)
        
        cursor = self.collector.connection.cursor()
        
        try:
            # Test daily summary calculation
            cursor.execute("EXEC market.sp_CalculateDailySummary")
            result = cursor.fetchone()
            
            if result:
                print(f"✓ Daily Summary: Processed {result[0]} spreads")
                
            self.collector.connection.commit()
            
        except Exception as e:
            print(f"✗ Error in analysis procedures: {e}")
            self.collector.connection.rollback()
            
        finally:
            cursor.close()
            
    def run_all_tests(self):
        """Run all system tests"""
        print("\nLME Metal Spreads SQL Server System Test")
        print("="*60)
        print(f"Test started at: {datetime.now()}")
        
        try:
            # Test connections
            if not self.test_database_connection():
                return
                
            if not self.test_bloomberg_connection():
                return
                
            # Test data collection
            spreads = self.test_spread_search('CU')
            
            if spreads:
                stored_spreads = self.test_data_storage(spreads)
                
                # Get market data
                market_data = self.test_market_data_collection(stored_spreads)
                
                if market_data:
                    self.test_tick_storage(market_data)
                    
            # Test views and analysis
            self.test_views()
            self.test_analysis_procedures()
            
            print("\n" + "="*60)
            print("All tests completed successfully!")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ Test failed with error: {e}")
            
        finally:
            self.collector.close()
            

def main():
    """Run system tests"""
    tester = SQLSystemTester()
    tester.run_all_tests()
    

if __name__ == "__main__":
    main()