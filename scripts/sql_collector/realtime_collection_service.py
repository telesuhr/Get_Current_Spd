"""
Real-time Data Collection Service for LME Metal Spreads
Version: 1.0
Date: 2025-07-18

This service runs continuously to collect real-time market data
at different frequencies based on configuration.
"""

import sys
import time
import signal
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from sql_data_collector import SQLServerDataCollector


class RealtimeCollectionService:
    """Service for continuous real-time data collection"""
    
    def __init__(self, config_path: str = "config.json"):
        self.collector = SQLServerDataCollector(config_path)
        self.running = False
        self.threads = []
        self.task_queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.collection_schedules = {}
        self.logger = logging.getLogger('RealtimeService')
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        
    def start(self):
        """Start the collection service"""
        self.logger.info("Starting Real-time Collection Service")
        
        # Connect to database and Bloomberg
        if not self.collector.connect_database():
            self.logger.error("Failed to connect to database")
            return False
            
        if not self.collector.start_bloomberg_session():
            self.logger.error("Failed to start Bloomberg session")
            return False
            
        # Load collection schedules
        self._load_collection_schedules()
        
        # Start collection threads
        self.running = True
        self._start_collection_threads()
        
        self.logger.info("Service started successfully")
        return True
        
    def _load_collection_schedules(self):
        """Load collection schedules from database"""
        cursor = self.collector.connection.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    cc.config_id,
                    m.metal_code,
                    cc.collection_type,
                    cc.interval_minutes,
                    cc.last_run,
                    cc.next_run
                FROM config.collection_config cc
                JOIN config.metals m ON cc.metal_id = m.metal_id
                WHERE cc.is_active = 1 AND m.is_active = 1
            """)
            
            for row in cursor.fetchall():
                config_id = row[0]
                self.collection_schedules[config_id] = {
                    'metal_code': row[1],
                    'collection_type': row[2],
                    'interval_minutes': row[3],
                    'last_run': row[4],
                    'next_run': row[5] or datetime.now()
                }
                
            self.logger.info(f"Loaded {len(self.collection_schedules)} collection schedules")
            
        finally:
            cursor.close()
            
    def _start_collection_threads(self):
        """Start threads for different collection types"""
        # High-frequency collection thread (active spreads)
        realtime_thread = threading.Thread(
            target=self._collection_worker,
            args=('REALTIME',),
            name='RealtimeCollector'
        )
        realtime_thread.start()
        self.threads.append(realtime_thread)
        
        # Regular collection thread (all spreads)
        regular_thread = threading.Thread(
            target=self._collection_worker,
            args=('REGULAR',),
            name='RegularCollector'
        )
        regular_thread.start()
        self.threads.append(regular_thread)
        
        # Daily maintenance thread
        daily_thread = threading.Thread(
            target=self._collection_worker,
            args=('DAILY',),
            name='DailyCollector'
        )
        daily_thread.start()
        self.threads.append(daily_thread)
        
    def _collection_worker(self, collection_type: str):
        """Worker thread for a specific collection type"""
        self.logger.info(f"Started {collection_type} collection worker")
        
        while self.running:
            try:
                # Check which schedules are due
                now = datetime.now()
                due_schedules = []
                
                for config_id, schedule in self.collection_schedules.items():
                    if (schedule['collection_type'] == collection_type and 
                        schedule['next_run'] <= now):
                        due_schedules.append((config_id, schedule))
                        
                # Process due schedules
                for config_id, schedule in due_schedules:
                    self._process_collection(config_id, schedule)
                    
                # Sleep for a short interval
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error in {collection_type} worker: {e}")
                time.sleep(30)  # Wait before retrying
                
    def _process_collection(self, config_id: int, schedule: Dict):
        """Process a single collection task"""
        metal_code = schedule['metal_code']
        collection_type = schedule['collection_type']
        
        self.logger.info(f"Starting {collection_type} collection for {metal_code}")
        
        try:
            if collection_type == 'REALTIME':
                self._collect_active_spreads(metal_code)
            elif collection_type == 'REGULAR':
                self._collect_all_spreads(metal_code)
            elif collection_type == 'DAILY':
                self._daily_maintenance(metal_code)
                
            # Update schedule
            self.collector.update_collection_status(metal_code, collection_type)
            
            # Update local schedule
            schedule['last_run'] = datetime.now()
            schedule['next_run'] = datetime.now() + timedelta(minutes=schedule['interval_minutes'])
            
            self.logger.info(f"Completed {collection_type} collection for {metal_code}")
            
        except Exception as e:
            self.logger.error(f"Error in {collection_type} collection for {metal_code}: {e}")
            
    def _collect_active_spreads(self, metal_code: str):
        """Collect data for active spreads only"""
        # Get spreads that have been active in the last hour
        active_spreads = self.collector.get_active_spreads(metal_code, hours=1)
        
        if not active_spreads:
            self.logger.info(f"No active spreads found for {metal_code}")
            return
            
        self.logger.info(f"Collecting data for {len(active_spreads)} active {metal_code} spreads")
        
        # Get market data
        market_data = self.collector.get_market_data(active_spreads)
        
        # Filter only spreads with current bid/ask
        current_data = [
            d for d in market_data 
            if d.get('BID') is not None or d.get('ASK') is not None
        ]
        
        # Store tick data
        if current_data:
            stored = self.collector.store_tick_data(current_data)
            self.logger.info(f"Stored {stored} tick records for active {metal_code} spreads")
            
    def _collect_all_spreads(self, metal_code: str):
        """Collect data for all spreads"""
        cursor = self.collector.connection.cursor()
        
        try:
            # Get all active spreads
            cursor.execute("""
                SELECT spread_id, ticker, spread_type, description
                FROM market.spreads s
                JOIN config.metals m ON s.metal_id = m.metal_id
                WHERE m.metal_code = ? AND s.is_active = 1
            """, (metal_code,))
            
            all_spreads = []
            for row in cursor.fetchall():
                all_spreads.append({
                    'spread_id': row[0],
                    'ticker': row[1],
                    'spread_type': row[2],
                    'description': row[3],
                    'metal_code': metal_code
                })
                
            self.logger.info(f"Collecting data for {len(all_spreads)} {metal_code} spreads")
            
            # Process in batches
            batch_size = 100
            for i in range(0, len(all_spreads), batch_size):
                batch = all_spreads[i:i+batch_size]
                
                # Get market data
                market_data = self.collector.get_market_data(batch)
                
                # Store tick data
                if market_data:
                    stored = self.collector.store_tick_data(market_data)
                    self.logger.info(f"Stored {stored} records for batch {i//batch_size + 1}")
                    
        finally:
            cursor.close()
            
    def _daily_maintenance(self, metal_code: str):
        """Perform daily maintenance tasks"""
        self.logger.info(f"Starting daily maintenance for {metal_code}")
        
        # 1. Search for new spreads
        self._search_new_spreads(metal_code)
        
        # 2. Update prompt dates
        self._update_prompt_dates(metal_code)
        
        # 3. Calculate daily summaries
        self._calculate_daily_summaries(metal_code)
        
        # 4. Mark inactive spreads
        self._mark_inactive_spreads(metal_code)
        
    def _search_new_spreads(self, metal_code: str):
        """Search for new spreads that may have been created"""
        self.logger.info(f"Searching for new {metal_code} spreads")
        
        # Search for spreads
        spreads = self.collector.search_spreads(metal_code)
        
        # Store any new spreads found
        if spreads:
            stored = self.collector.store_spreads(spreads)
            self.logger.info(f"Found and stored {stored} new {metal_code} spreads")
            
    def _update_prompt_dates(self, metal_code: str):
        """Update prompt dates for spreads"""
        # This would parse tickers and calculate/fetch prompt dates
        # Implementation depends on specific requirements
        pass
        
    def _calculate_daily_summaries(self, metal_code: str):
        """Calculate daily summary statistics"""
        cursor = self.collector.connection.cursor()
        
        try:
            cursor.execute("""
                EXEC market.sp_CalculateDailySummary 
                    @trading_date = NULL,
                    @metal_id = (SELECT metal_id FROM config.metals WHERE metal_code = ?)
            """, (metal_code,))
            
            result = cursor.fetchone()
            if result:
                self.logger.info(f"Calculated {result[0]} daily summaries for {metal_code}")
                
            self.collector.connection.commit()
            
        except Exception as e:
            self.logger.error(f"Error calculating daily summaries: {e}")
            self.collector.connection.rollback()
            
        finally:
            cursor.close()
            
    def _mark_inactive_spreads(self, metal_code: str):
        """Mark spreads as inactive if no data for extended period"""
        cursor = self.collector.connection.cursor()
        
        try:
            # Mark spreads inactive if no data for 30 days
            cursor.execute("""
                UPDATE s
                SET s.is_active = 0,
                    s.updated_at = GETDATE()
                FROM market.spreads s
                JOIN config.metals m ON s.metal_id = m.metal_id
                WHERE m.metal_code = ?
                AND s.is_active = 1
                AND NOT EXISTS (
                    SELECT 1 
                    FROM market.tick_data t 
                    WHERE t.spread_id = s.spread_id 
                    AND t.timestamp > DATEADD(DAY, -30, GETDATE())
                )
            """, (metal_code,))
            
            inactive_count = cursor.rowcount
            if inactive_count > 0:
                self.logger.info(f"Marked {inactive_count} {metal_code} spreads as inactive")
                
            self.collector.connection.commit()
            
        except Exception as e:
            self.logger.error(f"Error marking inactive spreads: {e}")
            self.collector.connection.rollback()
            
        finally:
            cursor.close()
            
    def stop(self):
        """Stop the collection service"""
        self.logger.info("Stopping Real-time Collection Service")
        
        self.running = False
        
        # Wait for threads to finish
        for thread in self.threads:
            thread.join(timeout=30)
            
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        # Close connections
        self.collector.close()
        
        self.logger.info("Service stopped")
        
    def run(self):
        """Run the service (blocking)"""
        if not self.start():
            return
            
        try:
            # Keep the main thread alive
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
            
        finally:
            self.stop()
            

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='LME Metal Spreads Real-time Collection Service')
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run service
    service = RealtimeCollectionService(args.config)
    service.run()
    

if __name__ == "__main__":
    main()