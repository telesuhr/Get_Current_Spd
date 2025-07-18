import blpapi
from datetime import datetime, date, timedelta
import pandas as pd
import time
import re

class RealtimeSpreadsFinder:
    """リアルタイムで全てのLME銅スプレッドを検索・取得（毎回最新のリストを作成）"""
    
    def __init__(self):
        self.session = None
        self.instrument_service = None
        self.refdata_service = None
        self.today = date.today()
        
        # 月コードマッピング
        self.month_codes = {
            'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
            'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
        }
        
        # 3M/Cashの満期日をキャッシュ
        self._three_month_prompt = None
        self._cash_prompt = None
        
    def start_session(self):
        options = blpapi.SessionOptions()
        options.setServerHost("localhost")
        options.setServerPort(8194)
        
        self.session = blpapi.Session(options)
        
        if not self.session.start():
            print("Failed to start session")
            return False
            
        if not self.session.openService("//blp/instruments"):
            print("Failed to open instruments service")
            return False
            
        if not self.session.openService("//blp/refdata"):
            print("Failed to open refdata service")
            return False
            
        self.instrument_service = self.session.getService("//blp/instruments")
        self.refdata_service = self.session.getService("//blp/refdata")
        
        # 3M/Cashの満期日を取得
        self._get_prompt_dates()
        
        return True
        
    def _get_prompt_dates(self):
        """実際のLME_PROMPT_DTをBloombergから取得"""
        try:
            # 3Mの満期日を取得
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            request.append("securities", "LMCADS03 Comdty")
            request.append("fields", "LME_PROMPT_DT")
            
            self.session.sendRequest(request)
            
            while True:
                event = self.session.nextEvent()
                
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
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
                                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # Cashの満期日を取得
            request2 = self.refdata_service.createRequest("ReferenceDataRequest")
            request2.append("securities", "LMCADY Comdty")
            request2.append("fields", "LME_PROMPT_DT")
            
            self.session.sendRequest(request2)
            
            while True:
                event = self.session.nextEvent()
                
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
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
                                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
        except Exception as e:
            print(f"Warning: Could not fetch LME_PROMPT_DT: {e}")
            # フォールバック
            self._three_month_prompt = self.today + timedelta(days=90)
            self._cash_prompt = self.today + timedelta(days=2)
            
    def search_all_spreads(self):
        """全てのLME銅スプレッドをリアルタイムで検索"""
        print("Searching for all LME Copper spreads in real-time...")
        all_spreads = []
        
        # 検索パターン（包括的に）
        search_patterns = [
            # 基本パターン
            "LMCADS",
            # 月コード（現在と将来の年）
            "LMCADS F", "LMCADS G", "LMCADS H", "LMCADS J",
            "LMCADS K", "LMCADS M", "LMCADS N", "LMCADS Q",
            "LMCADS U", "LMCADS V", "LMCADS X", "LMCADS Z",
            # 3M関連
            "LMCADS 03",
            # 特定日付（現在年と次年）
            "LMCADS 25", "LMCADS 26", "LMCADS 27",
            # Cash関連
            "LMCADS 00"
        ]
        
        # 月別の詳細検索（今年と来年）
        current_year = self.today.year
        for year in [current_year, current_year + 1]:
            year_str = str(year)[2:]  # 2025 -> 25
            for month in range(1, 13):
                search_patterns.append(f"LMCADS {year_str}{month:02d}")
        
        # 各パターンで検索
        seen_tickers = set()
        
        for pattern in search_patterns:
            request = self.instrument_service.createRequest("instrumentListRequest")
            request.set("query", pattern)
            request.set("yellowKeyFilter", "YK_FILTER_CMDT")
            request.set("maxResults", 1000)
            
            self.session.sendRequest(request)
            
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
                                    
                                    # スプレッドかどうかチェック
                                    if self._is_spread(security) and security not in seen_tickers:
                                        seen_tickers.add(security)
                                        description = result.getElementAsString("description") if result.hasElement("description") else ""
                                        
                                        # <cmdty>を Comdtyに変換
                                        ticker = security.replace('<cmdty>', ' Comdty')
                                        
                                        all_spreads.append({
                                            "ticker": ticker,
                                            "description": description,
                                            "spread_type": self._classify_spread_type(ticker)
                                        })
                                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
        print(f"Found {len(all_spreads)} unique spreads")
        return all_spreads
        
    def _is_spread(self, ticker):
        """スプレッドかどうかを判定"""
        # 単一の先物を除外
        if ticker == "LMCADS03<cmdty>" or ticker == "LMCADS<cmdty>":
            return False
            
        # スプレッドのパターン
        spread_patterns = [
            r'LMCADS\s+\d{6}-\d{6}',  # Odd-Odd
            r'LMCADS\s+\d{6}-03',      # Odd-3M
            r'LMCADS\s+03-\d{6}',      # 3M-Odd
            r'LMCADS\s+00-\d{6}',      # Cash-Odd
            r'LMCADS\s+\d{6}-00',      # Odd-Cash
            r'LMCADS\s+03[FGHJKMNQUVXZ]',  # 3M-3W
            r'LMCADS\s+[FGHJKMNQUVXZ]\d{2}[FGHJKMNQUVXZ]\d{2}',  # Calendar
            r'LMCADS\s+[FGHJKMNQUVXZ]\d{2}03',  # Month-3M
            r'LMCADS\s+00[FGHJKMNQUVXZ]',  # Cash-Month
        ]
        
        return any(re.search(pattern, ticker) for pattern in spread_patterns)
        
    def _classify_spread_type(self, ticker):
        """スプレッドのタイプを分類"""
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
            
    def get_market_data(self, spreads, batch_size=50):
        """市場データを取得"""
        market_data = []
        total_batches = (len(spreads) + batch_size - 1) // batch_size
        
        print(f"\nGetting market data for {len(spreads)} spreads...")
        print(f"Processing in {total_batches} batches...")
        
        for batch_num, i in enumerate(range(0, len(spreads), batch_size)):
            batch = spreads[i:i+batch_size]
            if batch_num % 5 == 0:  # 5バッチごとに進捗表示
                print(f"  Progress: {batch_num}/{total_batches} batches")
            
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            
            for spread in batch:
                request.append("securities", spread["ticker"])
                
            # フィールド
            request.append("fields", "BID")
            request.append("fields", "ASK")
            request.append("fields", "LAST_PRICE")
            request.append("fields", "VOLUME")
            request.append("fields", "LAST_UPDATE_DT")
            request.append("fields", "OPEN_INT")
            
            self.session.sendRequest(request)
            
            while True:
                event = self.session.nextEvent()
                
                if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    for msg in event:
                        if msg.hasElement("securityData"):
                            securityData = msg.getElement("securityData")
                            
                            for j in range(securityData.numValues()):
                                security = securityData.getValueAsElement(j)
                                ticker = security.getElementAsString("security")
                                
                                if security.hasElement("fieldData"):
                                    fieldData = security.getElement("fieldData")
                                    
                                    # 対応するspread情報を見つける
                                    spread_info = next((s for s in batch if s["ticker"] == ticker), None)
                                    if spread_info:
                                        data = spread_info.copy()
                                        
                                        data["bid"] = self._get_field_value(fieldData, "BID")
                                        data["ask"] = self._get_field_value(fieldData, "ASK")
                                        data["last_price"] = self._get_field_value(fieldData, "LAST_PRICE")
                                        data["volume"] = self._get_field_value(fieldData, "VOLUME")
                                        data["last_update"] = self._get_field_value(fieldData, "LAST_UPDATE_DT")
                                        data["open_interest"] = self._get_field_value(fieldData, "OPEN_INT")
                                        
                                        # 板があるものだけ追加
                                        if data["bid"] is not None or data["ask"] is not None:
                                            # 当日以外の出来高は0にする
                                            if data["last_update"] != str(self.today):
                                                data["volume"] = 0
                                            market_data.append(data)
                                            
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            time.sleep(0.3)  # レート制限
            
        return market_data
        
    def _get_field_value(self, fieldData, field_name):
        if fieldData.hasElement(field_name):
            element = fieldData.getElement(field_name)
            if element.datatype() == blpapi.DataType.FLOAT64:
                return element.getValueAsFloat()
            elif element.datatype() == blpapi.DataType.INT32:
                return element.getValueAsInteger()
            elif element.datatype() == blpapi.DataType.INT64:
                return element.getValueAsInteger()
            elif element.datatype() == blpapi.DataType.STRING:
                return element.getValueAsString()
            elif element.datatype() == blpapi.DataType.DATE:
                return element.getValueAsString()
        return None
        
    def parse_prompt_dates(self, ticker):
        """満期日を解析（簡易版）"""
        # ここは元のget_all_spreads_with_prompts.pyの実装を使用
        # 省略...
        return ("---", "---")
        
    def save_results(self, market_data):
        """結果を保存・表示"""
        if not market_data:
            print("\nNo active spreads found.")
            return
            
        df = pd.DataFrame(market_data)
        
        # ソート
        df = df.sort_values(['volume', 'spread_type'], ascending=[False, True])
        
        # 表示
        print("\n" + "="*120)
        print(f"REALTIME LME COPPER SPREADS (as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print("="*120)
        print(f"{'Ticker':40} | {'Type':10} | {'Bid':10} | {'Ask':10} | {'Last':10} | {'Volume':8} | {'Update':12}")
        print("-"*120)
        
        for _, row in df.head(30).iterrows():
            ticker = row['ticker'][:40]
            spread_type = row['spread_type']
            bid = f"{row['bid']:.2f}" if pd.notna(row['bid']) else "---"
            ask = f"{row['ask']:.2f}" if pd.notna(row['ask']) else "---"
            last = f"{row['last_price']:.2f}" if pd.notna(row['last_price']) else "---"
            volume = f"{int(row['volume'])}" if row['volume'] > 0 else "---"
            update = row['last_update'] if pd.notna(row['last_update']) else "---"
            
            print(f"{ticker:40} | {spread_type:10} | {bid:10} | {ask:10} | {last:10} | {volume:8} | {update:12}")
            
        if len(df) > 30:
            print(f"\n... and {len(df) - 30} more active spreads")
            
        # CSV保存
        filename = f"realtime_spreads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"\nTotal active spreads: {len(df)}")
        print(f"Results saved to: {filename}")
        
        # サマリー
        print("\nSummary by type:")
        print(df['spread_type'].value_counts())
        
        return df
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    finder = RealtimeSpreadsFinder()
    
    try:
        if not finder.start_session():
            print("Failed to start Bloomberg session")
            return
            
        # 全スプレッドをリアルタイムで検索
        all_spreads = finder.search_all_spreads()
        
        # 市場データを取得
        market_data = finder.get_market_data(all_spreads)
        
        # 結果を保存・表示
        finder.save_results(market_data)
        
    finally:
        finder.stop_session()

if __name__ == "__main__":
    main()