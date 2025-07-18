import blpapi
from datetime import datetime
import pandas as pd
from collections import Counter

class SpreadAnalyzer:
    def __init__(self):
        self.session = None
        self.instrument_service = None
        self.month_codes = {
            'F': 'Jan', 'G': 'Feb', 'H': 'Mar', 'J': 'Apr',
            'K': 'May', 'M': 'Jun', 'N': 'Jul', 'Q': 'Aug',
            'U': 'Sep', 'V': 'Oct', 'X': 'Nov', 'Z': 'Dec'
        }
        
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
        
    def analyze_all_copper_instruments(self):
        """全てのLMCADS関連商品を分析"""
        search_patterns = [
            "LMCADS",
            "LMCADS 03",
            "LMCADS F", "LMCADS G", "LMCADS H", "LMCADS J",
            "LMCADS K", "LMCADS M", "LMCADS N", "LMCADS Q",
            "LMCADS U", "LMCADS V", "LMCADS X", "LMCADS Z"
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
                                    
                                    instrument_type = self.classify_instrument(security, description)
                                    
                                    pattern_results.append({
                                        "ticker": security,
                                        "description": description,
                                        "type": instrument_type,
                                        "search_pattern": pattern
                                    })
                                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            print(f"  Found {len(pattern_results)} results")
            all_instruments.extend(pattern_results)
            
        # 重複を削除
        unique_instruments = {}
        for inst in all_instruments:
            ticker = inst["ticker"]
            if ticker not in unique_instruments:
                unique_instruments[ticker] = inst
                
        return list(unique_instruments.values())
        
    def classify_instrument(self, ticker, description):
        """商品の種類を分類"""
        ticker_upper = ticker.upper()
        desc_upper = description.upper()
        
        # 基本的な3ヶ月先物
        if ticker == "LMCADS03 Comdty":
            return "3-Month Rolling Future"
            
        # 特定の限月
        if "LP" in ticker and len(ticker.split()[0]) == 5:  # LPM24のような形式
            return "Specific Month Future"
            
        # 3M-3W スプレッド
        if "LMCADS 03" in ticker:
            return "3M-3W Spread"
            
        # カレンダースプレッド（月間スプレッド）
        if "LMCADS" in ticker and any(month in ticker for month in "FGHJKMNQUVXZ"):
            # 2つの月コードがあるかチェック
            month_count = sum(1 for char in ticker if char in "FGHJKMNQUVXZ")
            if month_count >= 2:
                return "Calendar Spread"
            elif month_count == 1:
                return "Single Month Related"
                
        # スプレッドと明記されているもの
        if "SPREAD" in desc_upper or "SPRD" in desc_upper:
            return "Spread (Description)"
            
        # その他のLMCADS関連
        if "LMCADS" in ticker:
            return "Other LMCADS"
            
        return "Unknown"
        
    def display_summary(self, instruments):
        """結果のサマリーを表示"""
        df = pd.DataFrame(instruments)
        
        print("\n" + "="*80)
        print("SUMMARY OF LME COPPER INSTRUMENTS FOUND")
        print("="*80)
        
        # タイプ別の集計
        type_counts = Counter(df['type'])
        print("\nInstrument Types:")
        for inst_type, count in type_counts.most_common():
            print(f"  {inst_type:30} : {count:4} instruments")
            
        print(f"\nTotal unique instruments: {len(instruments)}")
        
        # 各タイプのサンプルを表示
        print("\n" + "-"*80)
        print("SAMPLE INSTRUMENTS BY TYPE:")
        print("-"*80)
        
        for inst_type in type_counts:
            print(f"\n{inst_type}:")
            samples = df[df['type'] == inst_type].head(5)
            for _, row in samples.iterrows():
                print(f"  {row['ticker']:40} | {row['description'][:60]}")
                
        # CSVに保存
        df.to_csv("lme_copper_all_instruments.csv", index=False)
        print(f"\nDetailed results saved to: lme_copper_all_instruments.csv")
        
        return df
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    analyzer = SpreadAnalyzer()
    
    try:
        if not analyzer.start_session():
            print("Failed to start Bloomberg session")
            return
            
        print("Analyzing all LME Copper instruments...")
        instruments = analyzer.analyze_all_copper_instruments()
        
        df = analyzer.display_summary(instruments)
        
        # スプレッドだけを抽出
        spread_types = ["3M-3W Spread", "Calendar Spread", "Spread (Description)"]
        spreads_only = df[df['type'].isin(spread_types)]
        
        print(f"\n\nTotal spreads found: {len(spreads_only)}")
        spreads_only.to_csv("lme_copper_spreads_only.csv", index=False)
        print("Spreads only saved to: lme_copper_spreads_only.csv")
        
    finally:
        analyzer.stop_session()

if __name__ == "__main__":
    main()