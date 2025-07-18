"""
Update prompt dates for LME spreads in JCL database
Version: 1.0
Date: 2025-07-18

This script parses spread tickers and updates prompt dates and leg descriptions.
"""

import os
import re
from datetime import datetime, date, timedelta
import calendar
import pyodbc
import blpapi
from sql_data_collector_jcl import SQLServerDataCollectorJCL


class PromptDateUpdater:
    """Updates prompt dates for LME spreads"""
    
    def __init__(self, config_path=None):
        # Default to root directory config file
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.jcl.json')
            config_path = os.path.abspath(config_path)
            
        self.collector = SQLServerDataCollectorJCL(config_path)
        self.month_codes = {
            'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
            'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
        }
        
        # Cache for 3M and Cash prompt dates
        self._three_month_prompt = None
        self._cash_prompt = None
        
    def connect(self):
        """Connect to database and Bloomberg"""
        if not self.collector.connect_database():
            raise Exception("Failed to connect to database")
            
        if not self.collector.start_bloomberg_session():
            raise Exception("Failed to connect to Bloomberg")
            
    def get_special_prompt_dates(self):
        """Get 3M and Cash prompt dates from Bloomberg"""
        print("Getting 3M and Cash prompt dates from Bloomberg...")
        
        # Get 3M prompt date from LMCADS03
        request = self.collector.refdata_service.createRequest("ReferenceDataRequest")
        request.append("securities", "LMCADS03 Comdty")
        request.append("fields", "LME_PROMPT_DT")
        
        self.collector.session.sendRequest(request)
        
        while True:
            event = self.collector.session.nextEvent()
            
            if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                for msg in event:
                    if msg.hasElement("securityData"):
                        securityData = msg.getElement("securityData")
                        
                        for i in range(securityData.numValues()):
                            security = securityData.getValueAsElement(i)
                            
                            if security.hasElement("fieldData"):
                                fieldData = security.getElement("fieldData")
                                
                                if fieldData.hasElement("LME_PROMPT_DT"):
                                    prompt_str = fieldData.getElement("LME_PROMPT_DT").getValueAsString()
                                    year, month, day = map(int, prompt_str.split('-'))
                                    self._three_month_prompt = date(year, month, day)
                                    print(f"  3M prompt date: {self._three_month_prompt}")
                                    
            if event.eventType() == blpapi.Event.RESPONSE:
                break
                
        # Get Cash prompt date from LMCADY
        request2 = self.collector.refdata_service.createRequest("ReferenceDataRequest")
        request2.append("securities", "LMCADY Comdty")
        request2.append("fields", "LME_PROMPT_DT")
        
        self.collector.session.sendRequest(request2)
        
        while True:
            event = self.collector.session.nextEvent()
            
            if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                for msg in event:
                    if msg.hasElement("securityData"):
                        securityData = msg.getElement("securityData")
                        
                        for i in range(securityData.numValues()):
                            security = securityData.getValueAsElement(i)
                            
                            if security.hasElement("fieldData"):
                                fieldData = security.getElement("fieldData")
                                
                                if fieldData.hasElement("LME_PROMPT_DT"):
                                    prompt_str = fieldData.getElement("LME_PROMPT_DT").getValueAsString()
                                    year, month, day = map(int, prompt_str.split('-'))
                                    self._cash_prompt = date(year, month, day)
                                    print(f"  Cash prompt date: {self._cash_prompt}")
                                    
            if event.eventType() == blpapi.Event.RESPONSE:
                break
                
        # Fallback if Bloomberg fails
        if self._three_month_prompt is None:
            self._three_month_prompt = self.get_business_day(date.today() + timedelta(days=90))
            
        if self._cash_prompt is None:
            self._cash_prompt = self.get_business_day(date.today() + timedelta(days=2))
            
    def get_third_wednesday(self, year, month):
        """Get third Wednesday of a month"""
        # Get first day of month
        first_day = date(year, month, 1)
        
        # Find first Wednesday
        first_wednesday = first_day
        while first_wednesday.weekday() != 2:  # 2 = Wednesday
            first_wednesday += timedelta(days=1)
            
        # Third Wednesday is 14 days after first Wednesday
        third_wednesday = first_wednesday + timedelta(days=14)
        
        # Check if it's a business day (simplified - just check weekends)
        return self.get_business_day(third_wednesday)
        
    def get_business_day(self, target_date):
        """Get next business day if target is weekend"""
        while target_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            target_date += timedelta(days=1)
        return target_date
        
    def parse_yymmdd(self, date_str):
        """Parse YYMMDD format to date"""
        year = 2000 + int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        return date(year, month, day)
        
    def parse_prompt_dates(self, ticker):
        """Parse prompt dates from ticker"""
        ticker_upper = ticker.upper()
        
        # Remove ' COMDTY' suffix for parsing
        ticker_clean = ticker_upper.replace(' COMDTY', '')
        
        # YYMMDD-YYMMDD format (Odd-Odd)
        match = re.search(r'LMCADS (\d{6})-(\d{6})', ticker_clean)
        if match:
            date1 = self.parse_yymmdd(match.group(1))
            date2 = self.parse_yymmdd(match.group(2))
            return date1, date2, date1.strftime('%Y-%m-%d'), date2.strftime('%Y-%m-%d')
            
        # YYMMDD-03 format (Odd-3M)
        match = re.search(r'LMCADS (\d{6})-03', ticker_clean)
        if match:
            date1 = self.parse_yymmdd(match.group(1))
            date2 = self._three_month_prompt
            return date1, date2, date1.strftime('%Y-%m-%d'), '3M'
            
        # 03-YYMMDD format (3M-Odd)
        match = re.search(r'LMCADS 03-(\d{6})', ticker_clean)
        if match:
            date1 = self._three_month_prompt
            date2 = self.parse_yymmdd(match.group(1))
            return date1, date2, '3M', date2.strftime('%Y-%m-%d')
            
        # 00-YYMMDD format (Cash-Odd)
        match = re.search(r'LMCADS 00-(\d{6})', ticker_clean)
        if match:
            date1 = self._cash_prompt
            date2 = self.parse_yymmdd(match.group(1))
            return date1, date2, 'Cash', date2.strftime('%Y-%m-%d')
            
        # YYMMDD-00 format (Odd-Cash)
        match = re.search(r'LMCADS (\d{6})-00', ticker_clean)
        if match:
            date1 = self.parse_yymmdd(match.group(1))
            date2 = self._cash_prompt
            return date1, date2, date1.strftime('%Y-%m-%d'), 'Cash'
            
        # Calendar spreads (MYY MYY)
        match = re.search(r'LMCADS ([FGHJKMNQUVXZ])(\d{2})([FGHJKMNQUVXZ])(\d{2})', ticker_clean)
        if match:
            month1 = self.month_codes[match.group(1)]
            year1 = 2000 + int(match.group(2))
            month2 = self.month_codes[match.group(3)]
            year2 = 2000 + int(match.group(4))
            
            date1 = self.get_third_wednesday(year1, month1)
            date2 = self.get_third_wednesday(year2, month2)
            
            return date1, date2, f"{match.group(1)}{match.group(2)}", f"{match.group(3)}{match.group(4)}"
            
        # 3M-3W spreads (03MYY)
        match = re.search(r'LMCADS 03([FGHJKMNQUVXZ])(\d{2})', ticker_clean)
        if match:
            month = self.month_codes[match.group(1)]
            year = 2000 + int(match.group(2))
            
            date1 = self._three_month_prompt
            date2 = self.get_third_wednesday(year, month)
            
            return date1, date2, '3M', f"{match.group(1)}{match.group(2)}"
            
        # Month-3M spreads (MYY03)
        match = re.search(r'LMCADS ([FGHJKMNQUVXZ])(\d{2})03', ticker_clean)
        if match:
            month = self.month_codes[match.group(1)]
            year = 2000 + int(match.group(2))
            
            date1 = self.get_third_wednesday(year, month)
            date2 = self._three_month_prompt
            
            return date1, date2, f"{match.group(1)}{match.group(2)}", '3M'
            
        # 00MYY format (Cash-Month)
        match = re.search(r'LMCADS 00([FGHJKMNQUVXZ])(\d{2})', ticker_clean)
        if match:
            month = self.month_codes[match.group(1)]
            year = 2000 + int(match.group(2))
            
            date1 = self._cash_prompt
            date2 = self.get_third_wednesday(year, month)
            
            return date1, date2, 'Cash', f"{match.group(1)}{match.group(2)}"
            
        return None, None, None, None
        
    def update_spreads(self, metal_code='CU'):
        """Update prompt dates for spreads"""
        cursor = self.collector.connection.cursor()
        
        try:
            # Get all spreads without prompt dates
            cursor.execute("""
                SELECT s.spread_id, s.ticker, s.spread_type
                FROM lme_market.LME_M_spreads s
                JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
                WHERE m.metal_code = ?
                AND (s.prompt_date1 IS NULL OR s.prompt_date2 IS NULL)
            """, (metal_code,))
            
            spreads = cursor.fetchall()
            print(f"\nFound {len(spreads)} {metal_code} spreads without prompt dates")
            
            updated_count = 0
            
            for spread_id, ticker, spread_type in spreads:
                date1, date2, leg1_desc, leg2_desc = self.parse_prompt_dates(ticker)
                
                if date1 and date2:
                    cursor.execute("""
                        UPDATE lme_market.LME_M_spreads
                        SET prompt_date1 = ?,
                            prompt_date2 = ?,
                            leg1_description = ?,
                            leg2_description = ?,
                            updated_at = GETDATE()
                        WHERE spread_id = ?
                    """, (date1, date2, leg1_desc, leg2_desc, spread_id))
                    
                    updated_count += 1
                    
                    if updated_count % 100 == 0:
                        print(f"  Updated {updated_count} spreads...")
                        
            self.collector.connection.commit()
            print(f"\n✓ Updated {updated_count} spreads with prompt dates")
            
            # Show sample results
            cursor.execute("""
                SELECT TOP 10 ticker, prompt_date1, prompt_date2, 
                       leg1_description, leg2_description
                FROM lme_market.LME_M_spreads s
                JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
                WHERE m.metal_code = ?
                AND prompt_date1 IS NOT NULL
                ORDER BY spread_id DESC
            """, (metal_code,))
            
            print("\nSample updated spreads:")
            print(f"{'Ticker':35} {'Prompt1':12} {'Prompt2':12} {'Leg1':10} {'Leg2':10}")
            print("-"*92)
            
            for row in cursor.fetchall():
                ticker = row[0][:35]
                p1 = row[1].strftime('%Y-%m-%d') if row[1] else 'None'
                p2 = row[2].strftime('%Y-%m-%d') if row[2] else 'None'
                l1 = row[3] or ''
                l2 = row[4] or ''
                print(f"{ticker:35} {p1:12} {p2:12} {l1:10} {l2:10}")
                
        finally:
            cursor.close()
            
    def run(self):
        """Run the prompt date update process"""
        print("\n" + "="*60)
        print("LME SPREAD PROMPT DATE UPDATE")
        print("="*60)
        print(f"Started at: {datetime.now()}")
        
        try:
            # Connect
            print("\nConnecting to database and Bloomberg...")
            self.connect()
            print("✓ Connected")
            
            # Get special prompt dates
            self.get_special_prompt_dates()
            
            # Update copper spreads
            self.update_spreads('CU')
            
            # Optionally update other metals
            # self.update_spreads('AL')
            # self.update_spreads('ZN')
            
            print("\n" + "="*60)
            print("UPDATE COMPLETED SUCCESSFULLY!")
            print("="*60)
            
        except Exception as e:
            print(f"\nERROR: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            self.collector.close()
            print(f"\nFinished at: {datetime.now()}")


def main():
    updater = PromptDateUpdater()
    updater.run()


if __name__ == "__main__":
    main()