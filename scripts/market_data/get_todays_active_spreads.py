import blpapi
from datetime import datetime, date
import pandas as pd
import time

class TodaysActiveSpreadsFinder:
    def __init__(self):
        self.session = None
        self.refdata_service = None
        self.today = date.today()
        
    def start_session(self):
        options = blpapi.SessionOptions()
        options.setServerHost("localhost")
        options.setServerPort(8194)
        
        self.session = blpapi.Session(options)
        
        if not self.session.start():
            print("Failed to start session")
            return False
            
        if not self.session.openService("//blp/refdata"):
            print("Failed to open refdata service")
            return False
            
        self.refdata_service = self.session.getService("//blp/refdata")
        return True
        
    def load_spreads_from_csv(self, csv_file="lme_copper_spreads_only.csv"):
        """CSVファイルからスプレッドリストを読み込む"""
        try:
            df = pd.read_csv(csv_file)
            # <cmdty>を Comdtyに変換
            df['ticker'] = df['ticker'].str.replace('<cmdty>', ' Comdty', regex=False)
            return df['ticker'].tolist()
        except FileNotFoundError:
            print(f"File {csv_file} not found. Please run analyze_spreads.py first.")
            return []
            
    def check_todays_updates(self, tickers, batch_size=50):
        """今日更新があったかチェック"""
        todays_active = []
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        
        print(f"Checking {len(tickers)} spreads for today's updates...")
        print(f"Today's date: {self.today}")
        print(f"Processing in {total_batches} batches of {batch_size}...")
        
        for batch_num, i in enumerate(range(0, len(tickers), batch_size)):
            batch = tickers[i:i+batch_size]
            print(f"\nBatch {batch_num + 1}/{total_batches} ({len(batch)} instruments)")
            
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            
            for ticker in batch:
                request.append("securities", ticker)
                
            # 今日の更新をチェックするフィールド
            request.append("fields", "LAST_UPDATE_DT")
            request.append("fields", "LAST_UPDATE_TIME")
            request.append("fields", "TRADING_DT_REALTIME")
            request.append("fields", "PX_LAST")
            request.append("fields", "PX_BID")
            request.append("fields", "PX_ASK")
            request.append("fields", "VOLUME")
            request.append("fields", "RT_PX_CHG_NET_1D")
            request.append("fields", "RT_PX_CHG_PCT_1D")
            request.append("fields", "TIME")
            request.append("fields", "BID_UPDATE_STAMP_RT")
            request.append("fields", "ASK_UPDATE_STAMP_RT")
            request.append("fields", "TRADE_UPDATE_STAMP_RT")
            
            self.session.sendRequest(request)
            
            batch_results = []
            
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
                                    
                                    data = {
                                        "ticker": ticker,
                                        "last_update_dt": self._get_field_value(fieldData, "LAST_UPDATE_DT"),
                                        "last_update_time": self._get_field_value(fieldData, "LAST_UPDATE_TIME"),
                                        "trading_dt": self._get_field_value(fieldData, "TRADING_DT_REALTIME"),
                                        "px_last": self._get_field_value(fieldData, "PX_LAST"),
                                        "px_bid": self._get_field_value(fieldData, "PX_BID"),
                                        "px_ask": self._get_field_value(fieldData, "PX_ASK"),
                                        "volume": self._get_field_value(fieldData, "VOLUME"),
                                        "change_1d": self._get_field_value(fieldData, "RT_PX_CHG_NET_1D"),
                                        "change_pct_1d": self._get_field_value(fieldData, "RT_PX_CHG_PCT_1D"),
                                        "time": self._get_field_value(fieldData, "TIME"),
                                        "bid_update": self._get_field_value(fieldData, "BID_UPDATE_STAMP_RT"),
                                        "ask_update": self._get_field_value(fieldData, "ASK_UPDATE_STAMP_RT"),
                                        "trade_update": self._get_field_value(fieldData, "TRADE_UPDATE_STAMP_RT")
                                    }
                                    
                                    batch_results.append(data)
                                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # 今日の更新があるかチェック
            for result in batch_results:
                if self._has_todays_update(result):
                    todays_active.append(result)
                    
            print(f"  Found {len([r for r in batch_results if self._has_todays_update(r)])} with today's updates")
            
            # レート制限を避けるため少し待機
            time.sleep(0.5)
            
        return todays_active
        
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
            elif element.datatype() == blpapi.DataType.DATETIME:
                return element.getValueAsString()
        return None
        
    def _has_todays_update(self, data):
        """今日の更新があるかチェック"""
        today_str = self.today.strftime("%Y-%m-%d")
        
        # last_update_dtをチェック
        if data.get("last_update_dt"):
            if today_str in str(data["last_update_dt"]):
                return True
                
        # trading_dtをチェック
        if data.get("trading_dt"):
            if today_str in str(data["trading_dt"]):
                return True
                
        # タイムスタンプフィールドをチェック
        for field in ["bid_update", "ask_update", "trade_update"]:
            if data.get(field):
                if today_str in str(data[field]):
                    return True
                    
        # ボリュームがあって価格が存在する場合も含める
        if data.get("volume") and data["volume"] > 0:
            if data.get("px_last") or data.get("px_bid") or data.get("px_ask"):
                return True
                
        return False
        
    def save_results(self, active_spreads):
        """結果を保存して表示"""
        if not active_spreads:
            print("\nNo spreads with today's updates found.")
            return
            
        df = pd.DataFrame(active_spreads)
        
        # 表示用に整形
        df['has_bid'] = df['px_bid'].notna()
        df['has_ask'] = df['px_ask'].notna()
        df['has_trade'] = df['px_last'].notna()
        df['has_volume'] = (df['volume'] > 0) & df['volume'].notna()
        
        # ソート（ボリュームと価格の存在で）
        df['activity_score'] = (
            df['has_bid'].astype(int) + 
            df['has_ask'].astype(int) + 
            df['has_trade'].astype(int) + 
            df['has_volume'].astype(int)
        )
        
        df = df.sort_values(['activity_score', 'volume'], ascending=[False, False])
        
        # 結果を表示
        print("\n" + "="*120)
        print(f"SPREADS WITH TODAY'S UPDATES ({self.today})")
        print("="*120)
        
        display_cols = ['ticker', 'px_last', 'px_bid', 'px_ask', 'volume', 'change_1d', 'last_update_dt']
        
        for _, row in df.head(30).iterrows():
            ticker = row['ticker']
            px_last = f"{row['px_last']:.2f}" if pd.notna(row['px_last']) else "---"
            px_bid = f"{row['px_bid']:.2f}" if pd.notna(row['px_bid']) else "---"
            px_ask = f"{row['px_ask']:.2f}" if pd.notna(row['px_ask']) else "---"
            volume = f"{int(row['volume'])}" if pd.notna(row['volume']) and row['volume'] > 0 else "---"
            change = f"{row['change_1d']:+.2f}" if pd.notna(row['change_1d']) else "---"
            
            print(f"{ticker:30} | Last: {px_last:8} | Bid/Ask: {px_bid:8}/{px_ask:8} | Vol: {volume:6} | Chg: {change}")
            
        if len(df) > 30:
            print(f"\n... and {len(df) - 30} more active spreads")
            
        # CSVに保存
        filename = f"todays_active_spreads_{self.today}.csv"
        df.to_csv(filename, index=False)
        print(f"\nTotal spreads with today's updates: {len(df)}")
        print(f"Results saved to: {filename}")
        
        # サマリー
        print("\nSummary:")
        print(f"  With bid updates: {df['has_bid'].sum()}")
        print(f"  With ask updates: {df['has_ask'].sum()}")
        print(f"  With trade updates: {df['has_trade'].sum()}")
        print(f"  With volume: {df['has_volume'].sum()}")
        
        return df
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    finder = TodaysActiveSpreadsFinder()
    
    try:
        if not finder.start_session():
            print("Failed to start Bloomberg session")
            return
            
        # CSVからスプレッドリストを読み込む
        spreads = finder.load_spreads_from_csv()
        
        if not spreads:
            print("No spreads found. Please run analyze_spreads.py first.")
            return
            
        # 今日の更新をチェック
        active_spreads = finder.check_todays_updates(spreads)
        
        # 結果を保存・表示
        finder.save_results(active_spreads)
        
    finally:
        finder.stop_session()

if __name__ == "__main__":
    main()