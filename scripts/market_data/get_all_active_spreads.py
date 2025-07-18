import blpapi
from datetime import datetime, date
import pandas as pd
import time

class AllActiveSpreadsFinder:
    """全てのアクティブなLME銅スプレッド（通常のカレンダー/3M-3Wスプレッド + Odd dateスプレッド）を取得"""
    
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
        
    def load_all_spreads(self):
        """通常のスプレッドとOdd dateスプレッドの両方を読み込む"""
        all_spreads = []
        
        # 通常のスプレッドを読み込む
        try:
            df_regular = pd.read_csv("lme_copper_spreads_only.csv")
            df_regular['ticker'] = df_regular['ticker'].str.replace('<cmdty>', ' Comdty', regex=False)
            all_spreads.extend(df_regular['ticker'].tolist())
            print(f"Loaded {len(df_regular)} regular spreads")
        except FileNotFoundError:
            print("Regular spreads file not found")
            
        # Odd dateスプレッドを読み込む
        try:
            df_odd = pd.read_csv("lme_copper_odd_date_spreads.csv")
            df_odd['ticker'] = df_odd['ticker'].str.replace('<cmdty>', ' Comdty', regex=False)
            all_spreads.extend(df_odd['ticker'].tolist())
            print(f"Loaded {len(df_odd)} odd date spreads")
        except FileNotFoundError:
            print("Odd date spreads file not found")
            
        # 重複を削除
        all_spreads = list(set(all_spreads))
        print(f"Total unique spreads: {len(all_spreads)}")
        
        return all_spreads
        
    def get_current_market_data(self, tickers, batch_size=50):
        """現在の市場データを取得"""
        market_data = []
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        
        print(f"\nGetting current market data for {len(tickers)} spreads...")
        print(f"Today's date: {self.today}")
        print(f"Processing in {total_batches} batches of {batch_size}...")
        
        for batch_num, i in enumerate(range(0, len(tickers), batch_size)):
            batch = tickers[i:i+batch_size]
            if batch_num % 10 == 0:  # 10バッチごとに進捗表示
                print(f"\nBatch {batch_num + 1}-{min(batch_num + 10, total_batches)}/{total_batches}")
            
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            
            for ticker in batch:
                request.append("securities", ticker)
                
            # フィールド
            request.append("fields", "BID")
            request.append("fields", "ASK")
            request.append("fields", "LAST_PRICE")
            request.append("fields", "BID_SIZE")
            request.append("fields", "ASK_SIZE")
            request.append("fields", "VOLUME")
            request.append("fields", "TRADING_DT_REALTIME")
            request.append("fields", "LAST_UPDATE_DT")
            request.append("fields", "OPEN_INT")
            request.append("fields", "EXCH_CODE")
            
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
                                        "bid": self._get_field_value(fieldData, "BID"),
                                        "ask": self._get_field_value(fieldData, "ASK"),
                                        "last_price": self._get_field_value(fieldData, "LAST_PRICE"),
                                        "bid_size": self._get_field_value(fieldData, "BID_SIZE"),
                                        "ask_size": self._get_field_value(fieldData, "ASK_SIZE"),
                                        "volume": self._get_field_value(fieldData, "VOLUME"),
                                        "trading_dt": self._get_field_value(fieldData, "TRADING_DT_REALTIME"),
                                        "last_update": self._get_field_value(fieldData, "LAST_UPDATE_DT"),
                                        "open_interest": self._get_field_value(fieldData, "OPEN_INT"),
                                        "exchange": self._get_field_value(fieldData, "EXCH_CODE")
                                    }
                                    
                                    batch_results.append(data)
                                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # アクティブな板があるものをフィルタリング
            for result in batch_results:
                if self._has_active_market(result):
                    # last_updateが今日でない場合、volumeを0にする
                    if result.get("last_update") != str(self.today):
                        result["volume"] = 0
                    market_data.append(result)
                    
            # レート制限を避けるため少し待機
            time.sleep(0.3)
            
        print(f"\nTotal active spreads found: {len(market_data)}")
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
        
    def _has_active_market(self, data):
        """アクティブな市場データがあるかチェック"""
        has_bid = data.get("bid") is not None
        has_ask = data.get("ask") is not None
        
        if has_bid or has_ask:
            return True
            
        if data.get("last_price") is not None and data.get("open_interest", 0) > 0:
            return True
            
        return False
        
    def _calculate_todays_volume(self, data):
        """当日出来高を計算"""
        if data.get("last_update") == str(self.today) and data.get("volume"):
            return data["volume"]
        return 0
        
    def _classify_spread_type(self, ticker):
        """スプレッドのタイプを分類"""
        if "250" in ticker or "251" in ticker or "260" in ticker:
            if "-" in ticker:
                parts = ticker.split()
                if len(parts) > 1 and "-" in parts[1]:
                    date_parts = parts[1].split("-")
                    if len(date_parts[0]) == 6 and len(date_parts[1]) == 6:
                        return "Odd-Odd"
                    elif date_parts[1] == "03":
                        return "Odd-3M"
                    elif date_parts[0] == "03":
                        return "3M-Odd"
                    elif date_parts[0] == "00":
                        return "Cash-Odd"
                    elif date_parts[1] == "00":
                        return "Odd-Cash"
            return "Odd Date"
        elif " 03" in ticker:
            return "3M-3W"
        elif any(month in ticker for month in ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]):
            if ticker.count(" ") > 1:
                return "Calendar"
        return "Other"
        
    def save_results(self, market_data):
        """結果を保存して表示"""
        if not market_data:
            print("\nNo active market spreads found.")
            return
            
        df = pd.DataFrame(market_data)
        
        # 当日出来高を計算
        df['todays_volume'] = df.apply(self._calculate_todays_volume, axis=1)
        
        # スプレッドタイプを分類
        df['spread_type'] = df['ticker'].apply(self._classify_spread_type)
        
        # スプレッド（Bid-Ask差）を計算
        df['bid_ask_spread'] = None
        mask = df['bid'].notna() & df['ask'].notna()
        df.loc[mask, 'bid_ask_spread'] = df.loc[mask, 'ask'] - df.loc[mask, 'bid']
        
        # アクティビティスコアを計算
        df['activity_score'] = 0
        df.loc[df['bid'].notna(), 'activity_score'] += 1
        df.loc[df['ask'].notna(), 'activity_score'] += 1
        df.loc[df['last_price'].notna(), 'activity_score'] += 1
        df.loc[df['todays_volume'] > 0, 'activity_score'] += 2
        
        # ソート
        df = df.sort_values(['activity_score', 'todays_volume', 'bid_ask_spread'], 
                          ascending=[False, False, True])
        
        # 結果を表示
        print("\n" + "="*180)
        print(f"ALL ACTIVE LME COPPER SPREADS (as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print("="*180)
        print(f"{'Ticker':50} | {'Type':10} | {'Bid':10} | {'Ask':10} | {'Last':10} | {'Spread':8} | {'Today Vol':10} | {'Update':12}")
        print("-"*180)
        
        for _, row in df.head(50).iterrows():
            ticker = row['ticker']
            spread_type = row['spread_type']
            bid = f"{row['bid']:.2f}" if pd.notna(row['bid']) else "---"
            ask = f"{row['ask']:.2f}" if pd.notna(row['ask']) else "---"
            last = f"{row['last_price']:.2f}" if pd.notna(row['last_price']) else "---"
            spread = f"{row['bid_ask_spread']:.2f}" if pd.notna(row['bid_ask_spread']) else "---"
            today_vol = f"{int(row['todays_volume'])}" if row['todays_volume'] > 0 else "---"
            update = row['last_update'] if pd.notna(row['last_update']) else "---"
            
            print(f"{ticker:50} | {spread_type:10} | {bid:10} | {ask:10} | {last:10} | {spread:8} | {today_vol:10} | {update:12}")
            
        if len(df) > 50:
            print(f"\n... and {len(df) - 50} more active spreads")
            
        # CSVに保存
        filename = f"all_active_lme_copper_spreads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"\nTotal active spreads: {len(df)}")
        print(f"Results saved to: {filename}")
        
        # サマリー
        print("\nSummary by spread type:")
        type_summary = df.groupby('spread_type').agg({
            'ticker': 'count',
            'todays_volume': lambda x: (x > 0).sum()
        }).rename(columns={'ticker': 'total', 'todays_volume': 'with_today_volume'})
        print(type_summary)
        
        print(f"\nTotal with bid/ask: {(df['bid'].notna() & df['ask'].notna()).sum()}")
        print(f"Total with today's volume: {(df['todays_volume'] > 0).sum()}")
        
        return df
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    finder = AllActiveSpreadsFinder()
    
    try:
        if not finder.start_session():
            print("Failed to start Bloomberg session")
            return
            
        # 全てのスプレッドを読み込む
        all_spreads = finder.load_all_spreads()
        
        if not all_spreads:
            print("No spreads found. Please run analyze_spreads.py and analyze_odd_date_spreads.py first.")
            return
            
        # 現在の市場データを取得
        market_data = finder.get_current_market_data(all_spreads)
        
        # 結果を保存・表示
        finder.save_results(market_data)
        
    finally:
        finder.stop_session()

if __name__ == "__main__":
    main()