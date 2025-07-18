"""
SQL Server Data Collector for LME Metal Spreads
Version: 1.0
Date: 2025-07-18

This module provides the SQLServerDataCollector class for collecting
market data from Bloomberg and storing it in SQL Server.
"""

import pyodbc
import blpapi
import json
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional
import pandas as pd
from pathlib import Path
import time


class SQLServerDataCollector:
    """Collects LME metal spread data from Bloomberg and stores in SQL Server"""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize collector with configuration"""
        self.config = self._load_config(config_path)
        self.connection = None
        self.session = None
        self.refdata_service = None
        self.instrument_service = None
        self.logger = self._setup_logging()
        
        # Metal codes mapping
        self.metal_configs = {
            'CU': {'base': 'LMCADS', 'name': 'Copper'},
            'AL': {'base': 'LMAHDS', 'name': 'Aluminum'},
            'ZN': {'base': 'LMZSDS', 'name': 'Zinc'},
            'PB': {'base': 'LMPBDS', 'name': 'Lead'},
            'NI': {'base': 'LMNIDS', 'name': 'Nickel'},
            'SN': {'base': 'LMSNDS', 'name': 'Tin'}
        }
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file"""
        with open(config_path, 'r') as f:
            return json.load(f)
            
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        
        logger = logging.getLogger('SQLDataCollector')
        logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
        
        # Create logs directory if it doesn't exist
        log_file = log_config.get('file', 'logs/collector.log')
        Path(log_file).parent.mkdir(exist_ok=True)
        
        # File handler with rotation
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=log_config.get('max_bytes', 10485760),
            backupCount=log_config.get('backup_count', 5)
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
        
    def connect_database(self) -> bool:
        """Connect to SQL Server database"""
        try:
            db_config = self.config['database']
            
            if db_config.get('trusted_connection', True):
                conn_string = (
                    f"Driver={{{db_config['driver']}}};"
                    f"Server={db_config['server']};"
                    f"Database={db_config['database']};"
                    f"Trusted_Connection=yes;"
                )
            else:
                conn_string = (
                    f"Driver={{{db_config['driver']}}};"
                    f"Server={db_config['server']};"
                    f"Database={db_config['database']};"
                    f"UID={db_config['username']};"
                    f"PWD={db_config['password']};"
                )
                
            self.connection = pyodbc.connect(
                conn_string,
                timeout=db_config.get('connection_timeout', 30)
            )
            self.connection.timeout = db_config.get('query_timeout', 300)
            
            self.logger.info("Successfully connected to SQL Server")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
            
    def start_bloomberg_session(self) -> bool:
        """Start Bloomberg API session"""
        try:
            bb_config = self.config.get('bloomberg', {})
            
            options = blpapi.SessionOptions()
            options.setServerHost(bb_config.get('host', 'localhost'))
            options.setServerPort(bb_config.get('port', 8194))
            
            self.session = blpapi.Session(options)
            
            if not self.session.start():
                self.logger.error("Failed to start Bloomberg session")
                return False
                
            if not self.session.openService("//blp/refdata"):
                self.logger.error("Failed to open refdata service")
                return False
                
            if not self.session.openService("//blp/instruments"):
                self.logger.error("Failed to open instruments service")
                return False
                
            self.refdata_service = self.session.getService("//blp/refdata")
            self.instrument_service = self.session.getService("//blp/instruments")
            
            self.logger.info("Successfully started Bloomberg session")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start Bloomberg session: {e}")
            return False
            
    def search_spreads(self, metal_code: str, search_patterns: List[str] = None) -> List[Dict]:
        """Search for spreads of a specific metal"""
        if metal_code not in self.metal_configs:
            self.logger.error(f"Unknown metal code: {metal_code}")
            return []
            
        base_ticker = self.metal_configs[metal_code]['base']
        
        if search_patterns is None:
            # Default search patterns
            search_patterns = [
                base_ticker,
                f"{base_ticker} F", f"{base_ticker} G", f"{base_ticker} H",
                f"{base_ticker} J", f"{base_ticker} K", f"{base_ticker} M",
                f"{base_ticker} N", f"{base_ticker} Q", f"{base_ticker} U",
                f"{base_ticker} V", f"{base_ticker} X", f"{base_ticker} Z",
                f"{base_ticker} 03", f"{base_ticker} 00",
                f"{base_ticker} 25", f"{base_ticker} 26"
            ]
            
        all_spreads = []
        seen_tickers = set()
        
        for pattern in search_patterns:
            request = self.instrument_service.createRequest("instrumentListRequest")
            request.set("query", pattern)
            request.set("yellowKeyFilter", "YK_FILTER_CMDT")
            request.set("maxResults", 1000)
            
            self.session.sendRequest(request)
            
            while True:
                event = self.session.nextEvent()
                
                if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                    for msg in event:
                        if msg.hasElement("results"):
                            results = msg.getElement("results")
                            
                            for i in range(results.numValues()):
                                result = results.getValueAsElement(i)
                                
                                if result.hasElement("security"):
                                    ticker = result.getElementAsString("security")
                                    
                                    if ticker not in seen_tickers and self._is_spread(ticker):
                                        seen_tickers.add(ticker)
                                        
                                        spread_info = {
                                            'ticker': ticker.replace('<cmdty>', ' Comdty'),
                                            'metal_code': metal_code,
                                            'spread_type': self._classify_spread_type(ticker),
                                            'description': result.getElementAsString("description") 
                                                         if result.hasElement("description") else ""
                                        }
                                        
                                        all_spreads.append(spread_info)
                                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
        self.logger.info(f"Found {len(all_spreads)} spreads for {metal_code}")
        return all_spreads
        
    def _is_spread(self, ticker: str) -> bool:
        """Check if ticker represents a spread"""
        # Single futures contracts to exclude
        single_patterns = [
            r'LMCADS03<cmdty>$', r'LMAHDS03<cmdty>$', r'LMZSDS03<cmdty>$',
            r'LMPBDS03<cmdty>$', r'LMNIDS03<cmdty>$', r'LMSNDS03<cmdty>$',
            r'LMCADS<cmdty>$', r'LMAHDS<cmdty>$', r'LMZSDS<cmdty>$'
        ]
        
        import re
        for pattern in single_patterns:
            if re.match(pattern, ticker):
                return False
                
        # Check for spread patterns
        return any([
            '-' in ticker,  # Date spreads
            re.search(r'[FGHJKMNQUVXZ]\d{2}[FGHJKMNQUVXZ]\d{2}', ticker),  # Calendar
            re.search(r'03[FGHJKMNQUVXZ]', ticker),  # 3M-3W
            re.search(r'[FGHJKMNQUVXZ]\d{2}03', ticker),  # Month-3M
        ])
        
    def _classify_spread_type(self, ticker: str) -> str:
        """Classify spread type based on ticker pattern"""
        import re
        
        if re.search(r'\d{6}-\d{6}', ticker):
            return "Odd-Odd"
        elif re.search(r'\d{6}-03', ticker):
            return "Odd-3M"
        elif re.search(r'03-\d{6}', ticker):
            return "3M-Odd"
        elif re.search(r'00-\d{6}', ticker):
            return "Cash-Odd"
        elif re.search(r'\d{6}-00', ticker):
            return "Odd-Cash"
        elif re.search(r'03[FGHJKMNQUVXZ]\d{2}', ticker):
            return "3M-3W"
        elif re.search(r'[FGHJKMNQUVXZ]\d{2}[FGHJKMNQUVXZ]\d{2}', ticker):
            return "Calendar"
        elif re.search(r'[FGHJKMNQUVXZ]\d{2}03', ticker):
            return "Month-3M"
        else:
            return "Other"
            
    def store_spreads(self, spreads: List[Dict]) -> int:
        """Store spread definitions in database"""
        stored_count = 0
        cursor = self.connection.cursor()
        
        try:
            for spread in spreads:
                cursor.execute("""
                    DECLARE @spread_id INT;
                    EXEC market.sp_UpsertSpread 
                        @metal_code = ?,
                        @ticker = ?,
                        @spread_type = ?,
                        @description = ?,
                        @spread_id = @spread_id OUTPUT;
                    SELECT @spread_id;
                """, (
                    spread['metal_code'],
                    spread['ticker'],
                    spread['spread_type'],
                    spread.get('description', '')
                ))
                
                spread_id = cursor.fetchone()[0]
                spread['spread_id'] = spread_id
                stored_count += 1
                
            self.connection.commit()
            self.logger.info(f"Stored {stored_count} spread definitions")
            
        except Exception as e:
            self.logger.error(f"Error storing spreads: {e}")
            self.connection.rollback()
            
        finally:
            cursor.close()
            
        return stored_count
        
    def get_market_data(self, spreads: List[Dict], fields: List[str] = None) -> List[Dict]:
        """Get market data for spreads from Bloomberg"""
        if fields is None:
            fields = [
                "BID", "ASK", "LAST_PRICE", "BID_SIZE", "ASK_SIZE",
                "VOLUME", "TRADING_DT_REALTIME", "LAST_UPDATE_DT",
                "OPEN_INT", "RT_SPREAD_BP", "CONTRACT_VALUE"
            ]
            
        market_data = []
        batch_size = self.config.get('collection', {}).get('batch_size', 50)
        
        for i in range(0, len(spreads), batch_size):
            batch = spreads[i:i+batch_size]
            
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            
            for spread in batch:
                request.append("securities", spread['ticker'])
                
            for field in fields:
                request.append("fields", field)
                
            self.session.sendRequest(request)
            
            while True:
                event = self.session.nextEvent()
                
                if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                    for msg in event:
                        if msg.hasElement("securityData"):
                            securityData = msg.getElement("securityData")
                            
                            for j in range(securityData.numValues()):
                                security = securityData.getValueAsElement(j)
                                ticker = security.getElementAsString("security")
                                
                                if security.hasElement("fieldData"):
                                    fieldData = security.getElement("fieldData")
                                    
                                    # Find corresponding spread info
                                    spread_info = next((s for s in batch if s['ticker'] == ticker), None)
                                    
                                    if spread_info:
                                        data = {
                                            'spread_id': spread_info['spread_id'],
                                            'ticker': ticker,
                                            'timestamp': datetime.now()
                                        }
                                        
                                        # Extract field values
                                        for field in fields:
                                            if fieldData.hasElement(field):
                                                data[field] = self._get_field_value(fieldData, field)
                                                
                                        market_data.append(data)
                                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # Rate limiting
            time.sleep(0.3)
            
        return market_data
        
    def _get_field_value(self, fieldData, field_name):
        """Extract field value from Bloomberg field data"""
        if not fieldData.hasElement(field_name):
            return None
            
        element = fieldData.getElement(field_name)
        
        if element.datatype() == blpapi.DataType.FLOAT64:
            return element.getValueAsFloat()
        elif element.datatype() in [blpapi.DataType.INT32, blpapi.DataType.INT64]:
            return element.getValueAsInteger()
        elif element.datatype() == blpapi.DataType.STRING:
            return element.getValueAsString()
        elif element.datatype() == blpapi.DataType.DATE:
            return element.getValueAsString()
            
        return None
        
    def store_tick_data(self, market_data: List[Dict]) -> int:
        """Store market tick data in database"""
        stored_count = 0
        cursor = self.connection.cursor()
        
        try:
            for data in market_data:
                # Calculate today's volume
                todays_volume = data.get('VOLUME', 0)
                if data.get('LAST_UPDATE_DT') != str(date.today()):
                    todays_volume = 0
                    
                cursor.execute("""
                    EXEC market.sp_InsertTickData
                        @spread_id = ?,
                        @timestamp = ?,
                        @bid = ?,
                        @ask = ?,
                        @last_price = ?,
                        @bid_size = ?,
                        @ask_size = ?,
                        @volume = ?,
                        @todays_volume = ?,
                        @open_interest = ?,
                        @last_update_dt = ?,
                        @trading_dt = ?,
                        @rt_spread_bp = ?,
                        @contract_value = ?,
                        @duplicate_check = ?
                """, (
                    data['spread_id'],
                    data['timestamp'],
                    data.get('BID'),
                    data.get('ASK'),
                    data.get('LAST_PRICE'),
                    data.get('BID_SIZE'),
                    data.get('ASK_SIZE'),
                    data.get('VOLUME'),
                    todays_volume,
                    data.get('OPEN_INT'),
                    data.get('LAST_UPDATE_DT'),
                    data.get('TRADING_DT_REALTIME'),
                    data.get('RT_SPREAD_BP'),
                    data.get('CONTRACT_VALUE'),
                    self.config.get('collection', {}).get('duplicate_check', True)
                ))
                
                stored_count += 1
                
            self.connection.commit()
            self.logger.info(f"Stored {stored_count} tick records")
            
        except Exception as e:
            self.logger.error(f"Error storing tick data: {e}")
            self.connection.rollback()
            
        finally:
            cursor.close()
            
        return stored_count
        
    def get_active_spreads(self, metal_code: str, hours: int = 1) -> List[Dict]:
        """Get spreads that have been active in the last N hours"""
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("""
                SELECT DISTINCT
                    s.spread_id,
                    s.ticker,
                    s.spread_type,
                    s.description
                FROM market.spreads s
                JOIN config.metals m ON s.metal_id = m.metal_id
                JOIN market.tick_data t ON s.spread_id = t.spread_id
                WHERE m.metal_code = ?
                AND t.timestamp > DATEADD(HOUR, -?, GETDATE())
                AND (t.bid IS NOT NULL OR t.ask IS NOT NULL)
                AND s.is_active = 1
            """, (metal_code, hours))
            
            spreads = []
            for row in cursor.fetchall():
                spreads.append({
                    'spread_id': row[0],
                    'ticker': row[1],
                    'spread_type': row[2],
                    'description': row[3],
                    'metal_code': metal_code
                })
                
            return spreads
            
        finally:
            cursor.close()
            
    def update_collection_status(self, metal_code: str, collection_type: str):
        """Update last run time for collection config"""
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("""
                UPDATE config.collection_config
                SET last_run = GETDATE(),
                    next_run = DATEADD(MINUTE, interval_minutes, GETDATE())
                WHERE metal_id = (SELECT metal_id FROM config.metals WHERE metal_code = ?)
                AND collection_type = ?
            """, (metal_code, collection_type))
            
            self.connection.commit()
            
        except Exception as e:
            self.logger.error(f"Error updating collection status: {e}")
            self.connection.rollback()
            
        finally:
            cursor.close()
            
    def close(self):
        """Close all connections"""
        if self.session:
            self.session.stop()
            self.logger.info("Bloomberg session closed")
            
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")
            

def main():
    """Example usage of SQLServerDataCollector"""
    collector = SQLServerDataCollector()
    
    try:
        # Connect to database and Bloomberg
        if not collector.connect_database():
            return
            
        if not collector.start_bloomberg_session():
            return
            
        # Search for copper spreads
        spreads = collector.search_spreads('CU')
        
        # Store spread definitions
        collector.store_spreads(spreads)
        
        # Get active spreads
        active_spreads = collector.get_active_spreads('CU', hours=24)
        
        # Get market data
        market_data = collector.get_market_data(active_spreads)
        
        # Store tick data
        collector.store_tick_data(market_data)
        
        # Update collection status
        collector.update_collection_status('CU', 'REALTIME')
        
    finally:
        collector.close()
        

if __name__ == "__main__":
    main()