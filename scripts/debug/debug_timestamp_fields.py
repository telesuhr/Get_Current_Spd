import blpapi
from datetime import datetime, date

def debug_timestamp_fields():
    """タイムスタンプフィールドをデバッグ"""
    
    # セッション開始
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)
    
    session = blpapi.Session(options)
    
    if not session.start():
        print("Failed to start session")
        return
        
    if not session.openService("//blp/refdata"):
        print("Failed to open refdata service")
        return
        
    refdata_service = session.getService("//blp/refdata")
    
    # テスト用のティッカー（活発なもの）
    test_ticker = "LMCADS Q25U25 Comdty"
    
    request = refdata_service.createRequest("ReferenceDataRequest")
    request.append("securities", test_ticker)
    
    # すべての可能なタイムスタンプフィールド
    timestamp_fields = [
        "LAST_UPDATE_DT",
        "LAST_UPDATE_TIME",
        "LAST_UPDATE",
        "TRADING_DT_REALTIME",
        "LAST_TRADE_DATE_TIME_RT",
        "BID_UPDATE_STAMP_RT",
        "ASK_UPDATE_STAMP_RT",
        "TRADE_UPDATE_STAMP_RT",
        "LAST_UPDATE_TIME_RT",
        "RT_TIME_OF_TRADE",
        "RT_TIME_OF_BID_RT",
        "RT_TIME_OF_ASK_RT",
        "TIME",
        "REALTIME_UPDATE_STAMP",
        "RT_LAST_UPDATE_TIME",
        "LAST_PRICE_UPDATE_RT",
        "RT_PRICING_DATETIME",
        "BLOOMBERG_CLOSE_DT",
        "DATE_AND_TIME_TRADED",
        "BID_TIME",
        "ASK_TIME",
        "MID_TIME",
        "RT_BID_UPDATE_STAMP",
        "RT_ASK_UPDATE_STAMP",
        "RT_MID_UPDATE_STAMP"
    ]
    
    # 価格フィールド
    price_fields = [
        "LAST_PRICE",
        "PX_LAST",
        "BID",
        "ASK",
        "PX_BID",
        "PX_ASK",
        "RT_PX_BID",
        "RT_PX_ASK",
        "RT_LAST",
        "VOLUME",
        "VOLUME_THEO",
        "RT_VOLUME_THEO"
    ]
    
    for field in timestamp_fields + price_fields:
        request.append("fields", field)
        
    print(f"Testing ticker: {test_ticker}")
    print("="*100)
    print(f"Today's date: {date.today()}")
    print("-"*100)
    
    # リクエスト送信
    session.sendRequest(request)
    
    # レスポンス処理
    while True:
        event = session.nextEvent()
        
        if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
            for msg in event:
                if msg.hasElement("securityData"):
                    securityData = msg.getElement("securityData")
                    
                    for i in range(securityData.numValues()):
                        security = securityData.getValueAsElement(i)
                        
                        if security.hasElement("fieldData"):
                            fieldData = security.getElement("fieldData")
                            
                            print("\nTIMESTAMP FIELDS:")
                            print("-"*50)
                            for field in timestamp_fields:
                                if fieldData.hasElement(field):
                                    element = fieldData.getElement(field)
                                    value = None
                                    
                                    if element.datatype() == blpapi.DataType.FLOAT64:
                                        value = element.getValueAsFloat()
                                    elif element.datatype() == blpapi.DataType.INT32:
                                        value = element.getValueAsInteger()
                                    elif element.datatype() == blpapi.DataType.INT64:
                                        value = element.getValueAsInteger()
                                    elif element.datatype() == blpapi.DataType.STRING:
                                        value = element.getValueAsString()
                                    elif element.datatype() == blpapi.DataType.DATE:
                                        value = element.getValueAsString()
                                    elif element.datatype() == blpapi.DataType.DATETIME:
                                        value = element.getValueAsString()
                                        
                                    if value is not None:
                                        # 今日の日付が含まれているかチェック
                                        is_today = str(date.today()) in str(value)
                                        today_marker = " <-- TODAY" if is_today else ""
                                        print(f"{field:30}: {value}{today_marker}")
                                        
                            print("\nPRICE FIELDS:")
                            print("-"*50)
                            for field in price_fields:
                                if fieldData.hasElement(field):
                                    element = fieldData.getElement(field)
                                    value = None
                                    
                                    if element.datatype() == blpapi.DataType.FLOAT64:
                                        value = element.getValueAsFloat()
                                    elif element.datatype() == blpapi.DataType.INT32:
                                        value = element.getValueAsInteger()
                                    elif element.datatype() == blpapi.DataType.INT64:
                                        value = element.getValueAsInteger()
                                        
                                    if value is not None:
                                        print(f"{field:30}: {value}")
                                        
        if event.eventType() == blpapi.Event.RESPONSE:
            break
            
    session.stop()

if __name__ == "__main__":
    debug_timestamp_fields()