"""
Classify actual spread types based on date analysis
Version: 1.0
Date: 2025-07-18

This script analyzes spread dates to determine actual spread types
(e.g., identifying Cash-3W spreads that appear as Odd-Odd in tickers).
"""

import os
import re
from datetime import datetime, date, timedelta
import calendar
import pyodbc
import blpapi
from sql_data_collector_jcl import SQLServerDataCollectorJCL


class ActualSpreadClassifier:
    """Classifies spreads based on actual date characteristics"""
    
    def __init__(self, config_path=None):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.jcl.json')
            config_path = os.path.abspath(config_path)
            
        self.collector = SQLServerDataCollectorJCL(config_path)
        self.today = date.today()
        
        # Cache for special dates
        self._three_month_prompt = None
        self._cash_prompt = None
        
    def connect(self):
        """Connect to database and Bloomberg"""
        if not self.collector.connect_database():
            raise Exception("Failed to connect to database")
            
        if not self.collector.start_bloomberg_session():
            raise Exception("Failed to connect to Bloomberg")
            
    def get_special_dates_from_bloomberg(self):
        """Get current 3M and Cash prompt dates from Bloomberg"""
        print("Getting 3M and Cash prompt dates from Bloomberg...")
        
        try:
            # Get 3M prompt date
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
                    
            # Get Cash prompt date
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
                    
        except Exception as e:
            print(f"Warning: Could not get prompt dates from Bloomberg: {e}")
            # Fallback
            self._three_month_prompt = self.get_business_day(self.today + timedelta(days=90))
            self._cash_prompt = self.get_business_day(self.today + timedelta(days=2))
            
    def get_third_wednesday(self, year, month):
        """Get third Wednesday of a month"""
        first_day = date(year, month, 1)
        first_wednesday = first_day
        while first_wednesday.weekday() != 2:  # 2 = Wednesday
            first_wednesday += timedelta(days=1)
        third_wednesday = first_wednesday + timedelta(days=14)
        return self.get_business_day(third_wednesday)
        
    def get_business_day(self, target_date):
        """Get next business day if target is weekend"""
        while target_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            target_date += timedelta(days=1)
        return target_date
        
    def is_third_wednesday(self, check_date):
        """Check if a date is the third Wednesday of its month"""
        third_wed = self.get_third_wednesday(check_date.year, check_date.month)
        return check_date == third_wed
        
    def classify_date(self, check_date):
        """Classify a date as Cash, 3M, 3W (third Wednesday), or Odd"""
        # Check if it's Cash date (within 2 days of current cash prompt)
        if self._cash_prompt and abs((check_date - self._cash_prompt).days) <= 2:
            return 'Cash'
            
        # Check if it's 3M date (within 2 days of current 3M prompt)
        if self._three_month_prompt and abs((check_date - self._three_month_prompt).days) <= 2:
            return '3M'
            
        # Check if it's third Wednesday of any month
        if self.is_third_wednesday(check_date):
            return '3W'
            
        # Otherwise it's an odd date
        return 'Odd'
        
    def classify_spread(self, prompt_date1, prompt_date2, ticker):
        """Classify spread based on its dates"""
        if not prompt_date1 or not prompt_date2:
            return None, None, None, "Missing prompt dates"
            
        leg1_type = self.classify_date(prompt_date1)
        leg2_type = self.classify_date(prompt_date2)
        
        # Create actual spread type
        actual_spread_type = f"{leg1_type}-{leg2_type}"
        
        # Add notes for special cases
        notes = []
        if '250722-250820' in ticker and leg1_type == 'Cash' and leg2_type == '3W':
            notes.append("Cash/Aug25 spread with date notation")
            
        return actual_spread_type, leg1_type, leg2_type, '; '.join(notes) if notes else None
        
    def update_spreads(self, metal_code='CU', limit=None):
        """Update spreads with actual classification"""
        cursor = self.collector.connection.cursor()
        
        try:
            # Get spreads to classify
            query = """
                SELECT 
                    spread_id,
                    ticker,
                    spread_type,
                    prompt_date1,
                    prompt_date2
                FROM lme_market.LME_M_spreads s
                JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
                WHERE m.metal_code = ?
                AND prompt_date1 IS NOT NULL
                AND prompt_date2 IS NOT NULL
                AND actual_spread_type IS NULL
            """
            
            if limit:
                query += f" ORDER BY spread_id OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
                
            cursor.execute(query, (metal_code,))
            spreads = cursor.fetchall()
            
            print(f"\nFound {len(spreads)} {metal_code} spreads to classify")
            
            classified_count = 0
            reclassified_count = 0
            
            for spread_id, ticker, original_type, date1, date2 in spreads:
                actual_type, leg1_type, leg2_type, notes = self.classify_spread(date1, date2, ticker)
                
                if actual_type:
                    cursor.execute("""
                        UPDATE lme_market.LME_M_spreads
                        SET actual_spread_type = ?,
                            actual_leg1_type = ?,
                            actual_leg2_type = ?,
                            classification_notes = ?,
                            updated_at = GETDATE()
                        WHERE spread_id = ?
                    """, (actual_type, leg1_type, leg2_type, notes, spread_id))
                    
                    classified_count += 1
                    
                    # Check if reclassified
                    original_normalized = original_type.replace('Calendar', '3W-3W').replace('3M-3W', '3M-3W')
                    if original_type != actual_type and original_normalized != actual_type:
                        reclassified_count += 1
                        print(f"  Reclassified: {ticker[:40]:40} {original_type:12} → {actual_type:12}")
                        
                    if classified_count % 100 == 0:
                        print(f"  Processed {classified_count} spreads...")
                        
            self.collector.connection.commit()
            print(f"\n[OK] Classified {classified_count} spreads")
            print(f"  Reclassified: {reclassified_count}")
            
            # Show summary
            cursor.execute("""
                SELECT 
                    actual_spread_type,
                    COUNT(*) as count
                FROM lme_market.LME_M_spreads s
                JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
                WHERE m.metal_code = ?
                AND actual_spread_type IS NOT NULL
                GROUP BY actual_spread_type
                ORDER BY COUNT(*) DESC
            """, (metal_code,))
            
            print(f"\n{metal_code} Spread Classification Summary:")
            print(f"{'Type':15} {'Count':10}")
            print("-"*25)
            
            for row in cursor.fetchall():
                print(f"{row[0]:15} {row[1]:10}")
                
            # Show specific example
            cursor.execute("""
                SELECT TOP 5
                    ticker,
                    spread_type,
                    actual_spread_type,
                    prompt_date1,
                    prompt_date2,
                    classification_notes
                FROM lme_market.LME_M_spreads
                WHERE ticker LIKE '%250722-250820%'
                   OR (actual_spread_type = 'Cash-3W' AND spread_type = 'Odd-Odd')
                ORDER BY ticker
            """)
            
            examples = cursor.fetchall()
            if examples:
                print("\nExamples of Cash-3W spreads with date notation:")
                print(f"{'Ticker':40} {'Original':12} {'Actual':12} {'Date1':12} {'Date2':12}")
                print("-"*92)
                
                for row in examples:
                    ticker = row[0][:40]
                    orig = row[1]
                    actual = row[2]
                    d1 = row[3].strftime('%Y-%m-%d') if row[3] else ''
                    d2 = row[4].strftime('%Y-%m-%d') if row[4] else ''
                    print(f"{ticker:40} {orig:12} {actual:12} {d1:12} {d2:12}")
                    if row[5]:
                        print(f"  Note: {row[5]}")
                        
        finally:
            cursor.close()
            
    def verify_classification(self):
        """Show classification results for verification"""
        cursor = self.collector.connection.cursor()
        
        try:
            # Show reclassified spreads
            cursor.execute("""
                SELECT COUNT(*) 
                FROM lme_market.V_spread_classification_comparison
                WHERE classification_status = 'Reclassified'
            """)
            
            reclassified_count = cursor.fetchone()[0]
            print(f"\nTotal reclassified spreads: {reclassified_count}")
            
            if reclassified_count > 0:
                cursor.execute("""
                    SELECT TOP 20
                        metal_code,
                        ticker,
                        original_type,
                        actual_spread_type,
                        prompt_date1,
                        prompt_date2
                    FROM lme_market.V_spread_classification_comparison
                    WHERE classification_status = 'Reclassified'
                    ORDER BY ticker
                """)
                
                print("\nSample reclassified spreads:")
                print(f"{'Metal':6} {'Ticker':35} {'Original':12} {'Actual':12} {'Date1':12} {'Date2':12}")
                print("-"*105)
                
                for row in cursor.fetchall():
                    metal = row[0]
                    ticker = row[1][:35]
                    orig = row[2]
                    actual = row[3]
                    d1 = row[4].strftime('%Y-%m-%d') if row[4] else ''
                    d2 = row[5].strftime('%Y-%m-%d') if row[5] else ''
                    print(f"{metal:6} {ticker:35} {orig:12} {actual:12} {d1:12} {d2:12}")
                    
        finally:
            cursor.close()
            
    def run(self):
        """Run the classification process"""
        print("\n" + "="*60)
        print("ACTUAL SPREAD TYPE CLASSIFICATION")
        print("="*60)
        print(f"Started at: {datetime.now()}")
        
        try:
            # Connect
            print("\nConnecting to database and Bloomberg...")
            self.connect()
            print("[OK] Connected")
            
            # Get special dates
            self.get_special_dates_from_bloomberg()
            
            # Classify copper spreads
            self.update_spreads('CU')
            
            # Verify results
            self.verify_classification()
            
            print("\n" + "="*60)
            print("CLASSIFICATION COMPLETED SUCCESSFULLY!")
            print("="*60)
            
        except Exception as e:
            print(f"\nERROR: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            self.collector.close()
            print(f"\nFinished at: {datetime.now()}")


def main():
    classifier = ActualSpreadClassifier()
    classifier.run()


if __name__ == "__main__":
    main()