import blpapi
from datetime import datetime, date
import pandas as pd
import re
from collections import Counter

class OddDateSpreadAnalyzer:
    def __init__(self):
        self.session = None
        self.instrument_service = None
        
    def start_session(self):
        options = blpapi.SessionOptions()
        options.setServerHost("localhost")
        options.setServerPort(8194)
        
        self.session = blpapi.Session(options)
        
        if not self.session.start():
            return False
            
        if not self.session.openService("//blp/instruments"):
            return False
            
        self.instrument_service = self.session.getService("//blp/instruments")
        return True
        
    def search_odd_date_spreads(self):
        """Odd dateスプレッド（特定日付間）を検索"""
        
        # 様々な検索パターンを試す
        search_patterns = [
            "LMCADS 25",  # 2025年の日付を含む
            "LMCADS 26",  # 2026年の日付を含む
            "LMCADS 2507",  # 2025年7月
            "LMCADS 2508",  # 2025年8月
            "LMCADS 2509",  # 2025年9月
            "LMCADS 250722",  # 具体的な日付
            "LMCADS 250729",  # 具体的な日付
            "LMCADS 250722-",  # 日付範囲
            "LMCADS -250729",  # 日付範囲
            "LMCADS 03-25",  # 3M-specific date
            "LMCADS 00-25",  # Cash-specific date
            "LMCADS 250801",
            "LMCADS 250815",
            "LMCADS 250901",
            "LMCADS 251001"
        ]
        
        all_instruments = []
        
        for pattern in search_patterns:
            print(f"\nSearching for pattern: {pattern}")
            request = self.instrument_service.createRequest("instrumentListRequest")
            request.set("query", pattern)
            request.set("yellowKeyFilter", "YK_FILTER_CMDT")
            request.set("maxResults", 1000)
            
            self.session.sendRequest(request)
            
            pattern_results = []
            
            while True:
                event = self.session.nextEvent()
                
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    for msg in event:
                        if msg.hasElement("results"):
                            results = msg.getElement("results")
                            
                            for i in range(results.numValues()):
                                result = results.getValueAsElement(i)
                                
                                if result.hasElement("security"):
                                    security = result.getElementAsString("security")
                                    description = result.getElementAsString("description") if result.hasElement("description") else ""
                                    
                                    # Odd dateパターンをチェック
                                    if self._is_odd_date_spread(security):
                                        pattern_results.append({
                                            "ticker": security,
                                            "description": description,
                                            "search_pattern": pattern,
                                            "spread_type": self._classify_odd_spread(security)
                                        })
                                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            print(f"  Found {len(pattern_results)} odd date spreads")
            all_instruments.extend(pattern_results)
            
        # 重複を削除
        unique_instruments = {}
        for inst in all_instruments:
            ticker = inst["ticker"]
            if ticker not in unique_instruments:
                unique_instruments[ticker] = inst
                
        return list(unique_instruments.values())
        
    def _is_odd_date_spread(self, ticker):
        """Odd dateスプレッドかどうかを判定"""
        # YYMMDD-YYMMDD形式（例：250722-250729）
        pattern1 = r'LMCADS (\d{6})-(\d{6})'
        # YYMMDD-03形式（例：250722-03）
        pattern2 = r'LMCADS (\d{6})-03'
        # 03-YYMMDD形式（例：03-250722）
        pattern3 = r'LMCADS 03-(\d{6})'
        # YYMMDD単独形式
        pattern4 = r'LMCADS (\d{6})\s*<'
        # 00-YYMMDD形式（Cash-specific date）
        pattern5 = r'LMCADS 00-(\d{6})'
        # YYMMDD-00形式
        pattern6 = r'LMCADS (\d{6})-00'
        
        for pattern in [pattern1, pattern2, pattern3, pattern4, pattern5, pattern6]:
            if re.search(pattern, ticker):
                return True
                
        return False
        
    def _classify_odd_spread(self, ticker):
        """Odd spreadの種類を分類"""
        if re.search(r'LMCADS (\d{6})-(\d{6})', ticker):
            return "Odd-Odd Spread"
        elif re.search(r'LMCADS (\d{6})-03', ticker):
            return "Odd-3M Spread"
        elif re.search(r'LMCADS 03-(\d{6})', ticker):
            return "3M-Odd Spread"
        elif re.search(r'LMCADS 00-(\d{6})', ticker):
            return "Cash-Odd Spread"
        elif re.search(r'LMCADS (\d{6})-00', ticker):
            return "Odd-Cash Spread"
        elif re.search(r'LMCADS (\d{6})\s*<', ticker):
            return "Single Odd Date"
        else:
            return "Unknown Odd Type"
            
    def display_results(self, instruments):
        """結果を表示"""
        if not instruments:
            print("\nNo odd date spreads found.")
            return
            
        df = pd.DataFrame(instruments)
        
        print("\n" + "="*100)
        print("ODD DATE SPREADS FOUND")
        print("="*100)
        
        # タイプ別に集計
        type_counts = Counter(df['spread_type'])
        print("\nSpread Types:")
        for spread_type, count in type_counts.most_common():
            print(f"  {spread_type:20} : {count:4} spreads")
            
        print(f"\nTotal odd date spreads: {len(instruments)}")
        
        # タイプ別にサンプルを表示
        print("\n" + "-"*100)
        print("SAMPLES BY TYPE:")
        print("-"*100)
        
        for spread_type in type_counts:
            print(f"\n{spread_type}:")
            samples = df[df['spread_type'] == spread_type].head(10)
            for _, row in samples.iterrows():
                print(f"  {row['ticker']:50} | {row['description'][:40]}")
                
        # CSVに保存
        df.to_csv("lme_copper_odd_date_spreads.csv", index=False)
        print(f"\nResults saved to: lme_copper_odd_date_spreads.csv")
        
        return df
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    analyzer = OddDateSpreadAnalyzer()
    
    try:
        if not analyzer.start_session():
            print("Failed to start Bloomberg session")
            return
            
        print("Searching for LME Copper odd date spreads...")
        odd_spreads = analyzer.search_odd_date_spreads()
        
        df = analyzer.display_results(odd_spreads)
        
        # 特定の例を確認
        print("\n" + "="*100)
        print("CHECKING SPECIFIC EXAMPLE: LMCADS 250722-250729")
        print("="*100)
        
        if df is not None and len(df) > 0:
            example = df[df['ticker'].str.contains('250722-250729', case=False, na=False)]
            if len(example) > 0:
                print("Found:")
                for _, row in example.iterrows():
                    print(f"  {row['ticker']} | {row['description']}")
            else:
                print("Not found in search results. Trying direct lookup...")
                
                # 直接検索
                request = analyzer.instrument_service.createRequest("instrumentListRequest")
                request.set("query", "LMCADS 250722-250729")
                request.set("yellowKeyFilter", "YK_FILTER_CMDT")
                request.set("maxResults", 10)
                
                analyzer.session.sendRequest(request)
                
                while True:
                    event = analyzer.session.nextEvent()
                    
                    if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                        for msg in event:
                            if msg.hasElement("results"):
                                results = msg.getElement("results")
                                
                                if results.numValues() > 0:
                                    print("\nDirect search found:")
                                    for i in range(results.numValues()):
                                        result = results.getValueAsElement(i)
                                        if result.hasElement("security"):
                                            security = result.getElementAsString("security")
                                            desc = result.getElementAsString("description") if result.hasElement("description") else ""
                                            print(f"  {security} | {desc}")
                                else:
                                    print("Not found in direct search either.")
                                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        break
        
    finally:
        analyzer.stop_session()

if __name__ == "__main__":
    main()