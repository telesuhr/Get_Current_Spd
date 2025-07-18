import blpapi
from datetime import datetime, date
import pandas as pd
import time

class TodaysMarketDataFinder:
    def __init__(self):
        self.session = None
        self.refdata_service = None
        self.today = date.today()
        self.today_str = self.today.strftime("%Y-%m-%d")
        
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
            
    def check_todays_market_data(self, tickers, batch_size=50):
        """今日の市場データのみを取得"""
        todays_active = []
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        
        print(f"Checking {len(tickers)} spreads for today's market data...")
        print(f"Today's date: {self.today}")
        print(f"Processing in {total_batches} batches of {batch_size}...")
        
        for batch_num, i in enumerate(range(0, len(tickers), batch_size)):
            batch = tickers[i:i+batch_size]
            print(f"\nBatch {batch_num + 1}/{total_batches} ({len(batch)} instruments)")
            
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            
            for ticker in batch:
                request.append("securities", ticker)
                
            # リアルタイムのタイムスタンプ付きフィールドを使用
            request.append("fields", "LAST_PRICE")
            request.append("fields", "BID")
            request.append("fields", "ASK")
            request.append("fields", "LAST_TRADE_DATE_TIME_RT")
            request.append("fields", "BID_UPDATE_STAMP_RT")
            request.append("fields", "ASK_UPDATE_STAMP_RT")
            request.append("fields", "LAST_UPDATE_TIME_RT")
            request.append("fields", "RT_TIME_OF_TRADE")
            request.append("fields", "RT_TIME_OF_BID_RT")
            request.append("fields", "RT_TIME_OF_ASK_RT")
            request.append("fields", "VOLUME_THEO")
            request.append("fields", "RT_VOLUME_THEO")
            request.append("fields", "BID_SIZE")
            request.append("fields", "ASK_SIZE")
            request.append("fields", "LAST_TRADE_SIZE_RT")
            request.append("fields", "RT_TRADING_PERIOD")
            
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
                                        "last_price": self._get_field_value(fieldData, "LAST_PRICE"),
                                        "bid": self._get_field_value(fieldData, "BID"),
                                        "ask": self._get_field_value(fieldData, "ASK"),
                                        "last_trade_time": self._get_field_value(fieldData, "LAST_TRADE_DATE_TIME_RT"),
                                        "bid_update_time": self._get_field_value(fieldData, "BID_UPDATE_STAMP_RT"),
                                        "ask_update_time": self._get_field_value(fieldData, "ASK_UPDATE_STAMP_RT"),
                                        "last_update_time": self._get_field_value(fieldData, "LAST_UPDATE_TIME_RT"),
                                        "rt_time_trade": self._get_field_value(fieldData, "RT_TIME_OF_TRADE"),
                                        "rt_time_bid": self._get_field_value(fieldData, "RT_TIME_OF_BID_RT"),
                                        "rt_time_ask": self._get_field_value(fieldData, "RT_TIME_OF_ASK_RT"),
                                        "volume": self._get_field_value(fieldData, "VOLUME_THEO"),
                                        "rt_volume": self._get_field_value(fieldData, "RT_VOLUME_THEO"),
                                        "bid_size": self._get_field_value(fieldData, "BID_SIZE"),
                                        "ask_size": self._get_field_value(fieldData, "ASK_SIZE"),
                                        "last_size": self._get_field_value(fieldData, "LAST_TRADE_SIZE_RT"),
                                        "trading_period": self._get_field_value(fieldData, "RT_TRADING_PERIOD")
                                    }
                                    
                                    batch_results.append(data)
                                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # 今日のデータのみフィルタリング
            for result in batch_results:
                if self._has_todays_data(result):
                    todays_active.append(result)
                    
            print(f"  Found {len([r for r in batch_results if self._has_todays_data(r)])} with today's market data")
            
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
        
    def _has_todays_data(self, data):
        """今日のデータがあるかチェック（より厳格）"""
        has_today_trade = False
        has_today_bid = False
        has_today_ask = False
        
        # 取引タイムスタンプをチェック
        for field in ["last_trade_time", "rt_time_trade", "last_update_time"]:
            if data.get(field) and self.today_str in str(data[field]):
                has_today_trade = True
                break
                
        # Bidタイムスタンプをチェック
        for field in ["bid_update_time", "rt_time_bid"]:
            if data.get(field) and self.today_str in str(data[field]):
                has_today_bid = True
                break
                
        # Askタイムスタンプをチェック
        for field in ["ask_update_time", "rt_time_ask"]:
            if data.get(field) and self.today_str in str(data[field]):
                has_today_ask = True
                break
                
        # 少なくとも1つの今日のデータがあればTrue
        if has_today_trade or has_today_bid or has_today_ask:
            return True
            
        # タイムスタンプがない場合、現在のトレーディング期間をチェック
        if data.get("trading_period") == "TRADING":
            if data.get("bid") is not None or data.get("ask") is not None:
                return True
                
        return False
        
    def save_results(self, active_spreads):
        """結果を保存して表示"""
        if not active_spreads:
            print("\nNo spreads with today's market data found.")
            return
            
        df = pd.DataFrame(active_spreads)
        
        # タイムスタンプを解析して今日のデータかチェック
        for time_field in ["last_trade_time", "bid_update_time", "ask_update_time"]:
            if time_field in df.columns:
                df[f'{time_field}_is_today'] = df[time_field].apply(
                    lambda x: self.today_str in str(x) if pd.notna(x) else False
                )
        
        # アクティビティスコアを計算
        df['has_today_trade'] = df.get('last_trade_time_is_today', False)
        df['has_today_bid'] = df.get('bid_update_time_is_today', False)
        df['has_today_ask'] = df.get('ask_update_time_is_today', False)
        df['has_bid_price'] = df['bid'].notna()
        df['has_ask_price'] = df['ask'].notna()
        df['has_last_price'] = df['last_price'].notna()
        
        # 今日のデータのみフィルタリング
        df_today = df[df['has_today_trade'] | df['has_today_bid'] | df['has_today_ask']]
        
        # ソート
        df_today = df_today.sort_values(
            ['has_today_trade', 'has_today_bid', 'has_today_ask', 'volume'], 
            ascending=[False, False, False, False]
        )
        
        # 結果を表示
        print("\n" + "="*140)
        print(f"SPREADS WITH TODAY'S MARKET DATA ({self.today})")
        print("="*140)
        print(f"{'Ticker':30} | {'Last':8} | {'Bid':8} | {'Ask':8} | {'Volume':8} | {'Today Trade':12} | {'Today Bid':10} | {'Today Ask':10}")
        print("-"*140)
        
        for _, row in df_today.head(30).iterrows():
            ticker = row['ticker']
            last = f"{row['last_price']:.2f}" if pd.notna(row['last_price']) else "---"
            bid = f"{row['bid']:.2f}" if pd.notna(row['bid']) else "---"
            ask = f"{row['ask']:.2f}" if pd.notna(row['ask']) else "---"
            volume = f"{int(row['volume'])}" if pd.notna(row['volume']) and row['volume'] > 0 else "---"
            
            today_trade = "YES" if row.get('has_today_trade', False) else "NO"
            today_bid = "YES" if row.get('has_today_bid', False) else "NO"
            today_ask = "YES" if row.get('has_today_ask', False) else "NO"
            
            print(f"{ticker:30} | {last:8} | {bid:8} | {ask:8} | {volume:8} | {today_trade:12} | {today_bid:10} | {today_ask:10}")
            
        if len(df_today) > 30:
            print(f"\n... and {len(df_today) - 30} more spreads with today's data")
            
        # CSVに保存
        filename = f"todays_market_data_{self.today}.csv"
        df_today.to_csv(filename, index=False)
        print(f"\nTotal spreads with today's market data: {len(df_today)}")
        print(f"Results saved to: {filename}")
        
        # サマリー
        print("\nSummary:")
        print(f"  With today's trade: {df_today['has_today_trade'].sum()}")
        print(f"  With today's bid: {df_today['has_today_bid'].sum()}")
        print(f"  With today's ask: {df_today['has_today_ask'].sum()}")
        
        # 今日の取引があるもののみ
        df_trades_only = df_today[df_today['has_today_trade']]
        print(f"\nSpreads with today's trades: {len(df_trades_only)}")
        
        return df_today
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    finder = TodaysMarketDataFinder()
    
    try:
        if not finder.start_session():
            print("Failed to start Bloomberg session")
            return
            
        # CSVからスプレッドリストを読み込む
        spreads = finder.load_spreads_from_csv()
        
        if not spreads:
            print("No spreads found. Please run analyze_spreads.py first.")
            return
            
        # 今日の市場データをチェック
        active_spreads = finder.check_todays_market_data(spreads)
        
        # 結果を保存・表示
        finder.save_results(active_spreads)
        
    finally:
        finder.stop_session()

if __name__ == "__main__":
    main()