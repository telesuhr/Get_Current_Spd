import blpapi
from datetime import datetime, date, timedelta
import pandas as pd
import time
import calendar
import re

class AllSpreadsWithPrompts:
    """全てのアクティブなLME銅スプレッドを満期日付きで取得"""
    
    def __init__(self):
        self.session = None
        self.refdata_service = None
        self.today = date.today()
        
        # 月コードマッピング
        self.month_codes = {
            'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
            'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
        }
        
        # LME休場日（簡易版 - 実際はもっと複雑）
        self.lme_holidays = [
            date(2025, 1, 1),   # New Year's Day
            date(2025, 4, 18),  # Good Friday
            date(2025, 4, 21),  # Easter Monday
            date(2025, 5, 5),   # Early May Bank Holiday
            date(2025, 5, 26),  # Spring Bank Holiday
            date(2025, 8, 25),  # Summer Bank Holiday
            date(2025, 12, 25), # Christmas
            date(2025, 12, 26), # Boxing Day
        ]
        
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
            
        if not self.session.openService("//blp/refdata"):
            print("Failed to open refdata service")
            return False
            
        self.refdata_service = self.session.getService("//blp/refdata")
        
        # 3Mの満期日を取得
        self._get_prompt_dates()
        
        return True
        
    def get_third_wednesday(self, year, month):
        """指定月の第3水曜日を取得"""
        # 月の最初の日
        first_day = date(year, month, 1)
        # 最初の水曜日を見つける
        first_wednesday = first_day
        while first_wednesday.weekday() != 2:  # 2 = Wednesday
            first_wednesday += timedelta(days=1)
        # 第3水曜日は最初の水曜日から14日後
        third_wednesday = first_wednesday + timedelta(days=14)
        
        # LME休場日の場合は翌営業日に調整
        while self.is_lme_holiday(third_wednesday):
            third_wednesday += timedelta(days=1)
            
        return third_wednesday
        
    def is_lme_holiday(self, check_date):
        """LME休場日かどうかをチェック"""
        # 週末
        if check_date.weekday() >= 5:  # 5=土曜, 6=日曜
            return True
        # 祝日
        if check_date in self.lme_holidays:
            return True
        return False
        
    def get_business_day(self, target_date, days_ahead=0):
        """営業日を取得（休場日を考慮）"""
        result_date = target_date
        days_counted = 0
        
        while days_counted < days_ahead:
            result_date += timedelta(days=1)
            if not self.is_lme_holiday(result_date):
                days_counted += 1
                
        # 最終日が休場日の場合は翌営業日
        while self.is_lme_holiday(result_date):
            result_date += timedelta(days=1)
            
        return result_date
        
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
                                        # YYYY-MM-DD形式からdateオブジェクトに変換
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
                                        # YYYY-MM-DD形式からdateオブジェクトに変換
                                        year, month, day = map(int, prompt_str.split('-'))
                                        self._cash_prompt = date(year, month, day)
                                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            # フォールバック
            if self._cash_prompt is None:
                self._cash_prompt = self.get_business_day(self.today, 2)
            
        except Exception as e:
            print(f"Warning: Could not fetch LME_PROMPT_DT: {e}")
            # フォールバックとして計算した値を使う
            self._three_month_prompt = self.get_three_month_forward()
            self._cash_prompt = self.get_business_day(self.today, 2)
    
    def get_three_month_forward(self, base_date=None):
        """3ヶ月先の満期日を計算（フォールバック）"""
        # キャッシュされた値があればそれを使う
        if base_date is None and self._three_month_prompt is not None:
            return self._three_month_prompt
            
        if base_date is None:
            base_date = self.today
            
        # 3ヶ月後の同日
        year = base_date.year
        month = base_date.month + 3
        day = base_date.day
        
        if month > 12:
            month -= 12
            year += 1
            
        # 月末日の調整
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        if day > last_day:
            day = last_day
            
        three_month_date = date(year, month, day)
        
        # 営業日調整
        while self.is_lme_holiday(three_month_date):
            three_month_date += timedelta(days=1)
            
        return three_month_date
        
    def parse_prompt_dates(self, ticker):
        """ティッカーから満期日を解析（2つのタプルで返す）"""
        ticker_upper = ticker.upper()
        
        # YYMMDD-YYMMDD形式
        match = re.search(r'LMCADS (\d{6})-(\d{6})', ticker_upper)
        if match:
            date1 = self.parse_yymmdd(match.group(1))
            date2 = self.parse_yymmdd(match.group(2))
            return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
            
        # YYMMDD-03形式
        match = re.search(r'LMCADS (\d{6})-03', ticker_upper)
        if match:
            date1 = self.parse_yymmdd(match.group(1))
            date2 = self.get_three_month_forward()
            return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
            
        # 03-YYMMDD形式
        match = re.search(r'LMCADS 03-(\d{6})', ticker_upper)
        if match:
            date1 = self.get_three_month_forward()
            date2 = self.parse_yymmdd(match.group(1))
            return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
            
        # 00-YYMMDD形式（Cash）
        match = re.search(r'LMCADS 00-(\d{6})', ticker_upper)
        if match:
            date1 = self._cash_prompt if self._cash_prompt else self.get_business_day(self.today, 2)
            date2 = self.parse_yymmdd(match.group(1))
            return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
            
        # YYMMDD-00形式
        match = re.search(r'LMCADS (\d{6})-00', ticker_upper)
        if match:
            date1 = self.parse_yymmdd(match.group(1))
            date2 = self._cash_prompt if self._cash_prompt else self.get_business_day(self.today, 2)
            return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
            
        # 03MYY形式（3M-3W）
        match = re.search(r'LMCADS 03([FGHJKMNQUVXZ])(\d{2})', ticker_upper)
        if match:
            month_code = match.group(1)
            year = 2000 + int(match.group(2))
            month = self.month_codes.get(month_code, 0)
            if month > 0:
                date1 = self.get_three_month_forward()
                date2 = self.get_third_wednesday(year, month)
                return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
                
        # MYY MYY形式（カレンダースプレッド）
        match = re.search(r'LMCADS ([FGHJKMNQUVXZ])(\d{2})([FGHJKMNQUVXZ])(\d{2})', ticker_upper)
        if match:
            month1_code = match.group(1)
            year1 = 2000 + int(match.group(2))
            month1 = self.month_codes.get(month1_code, 0)
            
            month2_code = match.group(3)
            year2 = 2000 + int(match.group(4))
            month2 = self.month_codes.get(month2_code, 0)
            
            if month1 > 0 and month2 > 0:
                date1 = self.get_third_wednesday(year1, month1)
                date2 = self.get_third_wednesday(year2, month2)
                return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
                
        # V2503形式など
        match = re.search(r'LMCADS ([FGHJKMNQUVXZ])(\d{2})03', ticker_upper)
        if match:
            month_code = match.group(1)
            year = 2000 + int(match.group(2))
            month = self.month_codes.get(month_code, 0)
            if month > 0:
                date1 = self.get_third_wednesday(year, month)
                date2 = self.get_three_month_forward(date1)
                return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
                
        # Q2503形式など
        match = re.search(r'LMCADS ([FGHJKMNQUVXZ])2503', ticker_upper)
        if match:
            month_code = match.group(1)
            month = self.month_codes.get(month_code, 0)
            if month > 0:
                # 2025年と仮定
                date1 = self.get_third_wednesday(2025, month)
                date2 = self.get_three_month_forward(date1)
                return (date1.strftime('%Y/%m/%d'), date2.strftime('%Y/%m/%d'))
                
        return ("---", "---")
        
    def parse_yymmdd(self, date_str):
        """YYMMDD形式の日付をパース"""
        try:
            year = 2000 + int(date_str[0:2])
            month = int(date_str[2:4])
            day = int(date_str[4:6])
            return date(year, month, day)
        except:
            return self.today
            
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
                    # 満期日を追加
                    prompt1, prompt2 = self.parse_prompt_dates(result["ticker"])
                    result["prompt_date1"] = prompt1
                    result["prompt_date2"] = prompt2
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
        print("\n" + "="*220)
        print(f"ALL ACTIVE LME COPPER SPREADS WITH PROMPT DATES (as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print("="*220)
        print(f"{'Ticker':40} | {'Prompt1':12} | {'Prompt2':12} | {'Type':10} | {'Bid':10} | {'Ask':10} | {'Last':10} | {'Spread':8} | {'Today Vol':10}")
        print("-"*220)
        
        for _, row in df.head(50).iterrows():
            ticker = row['ticker'][:40]
            prompt1 = row['prompt_date1']
            prompt2 = row['prompt_date2']
            spread_type = row['spread_type']
            bid = f"{row['bid']:.2f}" if pd.notna(row['bid']) else "---"
            ask = f"{row['ask']:.2f}" if pd.notna(row['ask']) else "---"
            last = f"{row['last_price']:.2f}" if pd.notna(row['last_price']) else "---"
            spread = f"{row['bid_ask_spread']:.2f}" if pd.notna(row['bid_ask_spread']) else "---"
            today_vol = f"{int(row['todays_volume'])}" if row['todays_volume'] > 0 else "---"
            
            print(f"{ticker:40} | {prompt1:12} | {prompt2:12} | {spread_type:10} | {bid:10} | {ask:10} | {last:10} | {spread:8} | {today_vol:10}")
            
        if len(df) > 50:
            print(f"\n... and {len(df) - 50} more active spreads")
            
        # CSVに保存
        filename = f"all_spreads_with_prompts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
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
    finder = AllSpreadsWithPrompts()
    
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