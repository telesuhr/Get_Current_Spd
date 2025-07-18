import blpapi
from datetime import datetime, date
import pandas as pd
import time

class TodaysVolumeSpreadsFinder:
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
            
    def get_current_market_data(self, tickers, batch_size=50):
        """現在の市場データを取得（当日出来高に焦点）"""
        market_data = []
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        
        print(f"Getting current market data for {len(tickers)} spreads...")
        print(f"Today's date: {self.today}")
        print(f"Processing in {total_batches} batches of {batch_size}...")
        
        for batch_num, i in enumerate(range(0, len(tickers), batch_size)):
            batch = tickers[i:i+batch_size]
            print(f"\nBatch {batch_num + 1}/{total_batches} ({len(batch)} instruments)")
            
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            
            for ticker in batch:
                request.append("securities", ticker)
                
            # 現在の板情報と当日出来高に関するフィールド
            request.append("fields", "BID")
            request.append("fields", "ASK")
            request.append("fields", "LAST_PRICE")
            request.append("fields", "BID_SIZE")
            request.append("fields", "ASK_SIZE")
            request.append("fields", "VOLUME")  # 累積出来高
            request.append("fields", "RT_VOLUME_THEO")  # リアルタイム理論出来高
            request.append("fields", "VOLUME_THEO")  # 理論出来高
            request.append("fields", "TODAY_VOLUME")  # 当日出来高（もし存在すれば）
            request.append("fields", "RT_TODAYS_VOLUME")  # リアルタイム当日出来高
            request.append("fields", "TRADING_DT_REALTIME")
            request.append("fields", "LAST_UPDATE_DT")
            request.append("fields", "RT_SPREAD_BP")
            request.append("fields", "OPEN_INT")
            request.append("fields", "CONTRACT_VALUE")
            request.append("fields", "RT_TRADING_PERIOD")
            request.append("fields", "PREV_CLOSE_VALUE_REALTIME")  # 前日終値
            request.append("fields", "TRADING_DAY_VOLUME")  # 取引日出来高
            request.append("fields", "THEO_VOLUME_TODAY_RT")  # 理論的な当日出来高
            
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
                                        "rt_volume_theo": self._get_field_value(fieldData, "RT_VOLUME_THEO"),
                                        "volume_theo": self._get_field_value(fieldData, "VOLUME_THEO"),
                                        "today_volume": self._get_field_value(fieldData, "TODAY_VOLUME"),
                                        "rt_todays_volume": self._get_field_value(fieldData, "RT_TODAYS_VOLUME"),
                                        "trading_day_volume": self._get_field_value(fieldData, "TRADING_DAY_VOLUME"),
                                        "theo_volume_today": self._get_field_value(fieldData, "THEO_VOLUME_TODAY_RT"),
                                        "trading_dt": self._get_field_value(fieldData, "TRADING_DT_REALTIME"),
                                        "last_update": self._get_field_value(fieldData, "LAST_UPDATE_DT"),
                                        "spread_bp": self._get_field_value(fieldData, "RT_SPREAD_BP"),
                                        "open_interest": self._get_field_value(fieldData, "OPEN_INT"),
                                        "contract_value": self._get_field_value(fieldData, "CONTRACT_VALUE"),
                                        "trading_period": self._get_field_value(fieldData, "RT_TRADING_PERIOD"),
                                        "prev_close": self._get_field_value(fieldData, "PREV_CLOSE_VALUE_REALTIME")
                                    }
                                    
                                    batch_results.append(data)
                                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # アクティブな板があるものをフィルタリング
            active_count = 0
            for result in batch_results:
                if self._has_active_market(result):
                    # last_updateが今日でない場合、volumeを0にする
                    if result.get("last_update") != str(self.today):
                        result["volume"] = 0
                    market_data.append(result)
                    active_count += 1
                    
            print(f"  Found {active_count} with active bid/ask")
            
            # レート制限を避けるため少し待機
            time.sleep(0.5)
            
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
        # Bid/Askの少なくとも一方が存在
        has_bid = data.get("bid") is not None
        has_ask = data.get("ask") is not None
        
        # 板情報があればアクティブとみなす
        if has_bid or has_ask:
            return True
            
        # 最近の取引があり、建玉がある場合もアクティブ
        if data.get("last_price") is not None and data.get("open_interest", 0) > 0:
            return True
            
        return False
        
    def _calculate_todays_volume(self, data):
        """当日出来高を計算"""
        # last_updateが今日（2025-07-18）の場合のみ、volumeを当日出来高とする
        if data.get("last_update") == str(self.today) and data.get("volume"):
            return data["volume"]
            
        # それ以外は0（過去の出来高を含めない）
        return 0
        
    def save_results(self, market_data):
        """結果を保存して表示"""
        if not market_data:
            print("\nNo active market spreads found.")
            return
            
        df = pd.DataFrame(market_data)
        
        # 当日出来高を計算
        df['todays_volume'] = df.apply(self._calculate_todays_volume, axis=1)
        
        # スプレッド（Bid-Ask差）を計算
        df['bid_ask_spread'] = None
        mask = df['bid'].notna() & df['ask'].notna()
        df.loc[mask, 'bid_ask_spread'] = df.loc[mask, 'ask'] - df.loc[mask, 'bid']
        
        # アクティビティスコアを計算
        df['activity_score'] = 0
        df.loc[df['bid'].notna(), 'activity_score'] += 1
        df.loc[df['ask'].notna(), 'activity_score'] += 1
        df.loc[df['last_price'].notna(), 'activity_score'] += 1
        df.loc[df['todays_volume'] > 0, 'activity_score'] += 2  # 当日出来高は重要
        df.loc[df['open_interest'] > 100, 'activity_score'] += 1
        
        # ソート（アクティビティスコアと当日出来高で）
        df = df.sort_values(['activity_score', 'todays_volume', 'open_interest'], 
                          ascending=[False, False, False])
        
        # 結果を表示
        print("\n" + "="*160)
        print(f"CURRENT ACTIVE MARKET SPREADS (as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print("="*160)
        print(f"{'Ticker':30} | {'Bid':10} | {'Ask':10} | {'Last':10} | {'Spread':8} | {'Today Vol':10} | {'Total Vol':10} | {'OI':8} | {'Update':12}")
        print("-"*160)
        
        for _, row in df.head(50).iterrows():
            ticker = row['ticker']
            bid = f"{row['bid']:.2f}" if pd.notna(row['bid']) else "---"
            ask = f"{row['ask']:.2f}" if pd.notna(row['ask']) else "---"
            last = f"{row['last_price']:.2f}" if pd.notna(row['last_price']) else "---"
            spread = f"{row['bid_ask_spread']:.2f}" if pd.notna(row['bid_ask_spread']) else "---"
            today_vol = f"{int(row['todays_volume'])}" if row['todays_volume'] > 0 else "---"
            total_vol = f"{int(row['volume'])}" if pd.notna(row['volume']) and row['volume'] > 0 else "---"
            oi = f"{int(row['open_interest'])}" if pd.notna(row['open_interest']) and row['open_interest'] > 0 else "---"
            update = row['last_update'] if pd.notna(row['last_update']) else "---"
            
            print(f"{ticker:30} | {bid:10} | {ask:10} | {last:10} | {spread:8} | {today_vol:10} | {total_vol:10} | {oi:8} | {update:12}")
            
        if len(df) > 50:
            print(f"\n... and {len(df) - 50} more active spreads")
            
        # CSVに保存
        filename = f"current_active_spreads_with_todays_volume_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"\nTotal active market spreads: {len(df)}")
        print(f"Results saved to: {filename}")
        
        # サマリー
        print("\nSummary:")
        print(f"  With bid: {df['bid'].notna().sum()}")
        print(f"  With ask: {df['ask'].notna().sum()}")
        print(f"  With both bid and ask: {(df['bid'].notna() & df['ask'].notna()).sum()}")
        print(f"  With today's volume: {(df['todays_volume'] > 0).sum()}")
        print(f"  With recent trades: {df['last_price'].notna().sum()}")
        
        # 今日取引があったもの
        today_str = self.today.strftime("%Y-%m-%d")
        df_today_trade = df[df['last_update'] == today_str]
        print(f"\nWith today's last update: {len(df_today_trade)}")
        
        # 当日出来高トップ10
        df_today_vol = df[df['todays_volume'] > 0].head(10)
        if len(df_today_vol) > 0:
            print("\nTop 10 by today's volume:")
            for _, row in df_today_vol.iterrows():
                print(f"  {row['ticker']:30} : {int(row['todays_volume']):6} lots")
        
        return df
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    finder = TodaysVolumeSpreadsFinder()
    
    try:
        if not finder.start_session():
            print("Failed to start Bloomberg session")
            return
            
        # CSVからスプレッドリストを読み込む
        spreads = finder.load_spreads_from_csv()
        
        if not spreads:
            print("No spreads found. Please run analyze_spreads.py first.")
            return
            
        # 現在の市場データを取得
        market_data = finder.get_current_market_data(spreads)
        
        # 結果を保存・表示
        finder.save_results(market_data)
        
    finally:
        finder.stop_session()

if __name__ == "__main__":
    main()