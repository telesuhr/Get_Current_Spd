import blpapi
from datetime import datetime, date

def debug_single_spread(ticker):
    """単一のスプレッドの詳細情報を取得してデバッグ"""
    
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
    
    # リクエスト作成
    request = refdata_service.createRequest("ReferenceDataRequest")
    
    # 異なる形式で試す
    test_tickers = [
        ticker,
        ticker.replace("<cmdty>", " Comdty"),
        ticker.replace("<cmdty>", " COMDTY"),
        "LMCADS Q25U25 Comdty"
    ]
    
    print(f"Testing ticker formats for: {ticker}")
    print("="*80)
    
    for test_ticker in test_tickers:
        request.append("securities", test_ticker)
        
    # 全てのフィールドを取得
    fields = [
        "LAST_UPDATE_DT",
        "LAST_UPDATE_TIME",
        "TRADING_DT_REALTIME",
        "PX_LAST",
        "PX_BID",
        "PX_ASK",
        "VOLUME",
        "RT_PX_CHG_NET_1D",
        "TIME",
        "BID_UPDATE_STAMP_RT",
        "ASK_UPDATE_STAMP_RT",
        "TRADE_UPDATE_STAMP_RT",
        "LAST_PRICE_UPDATE_RT",
        "RT_TIME_OF_LAST_UPDATE",
        "LAST_UPDATE",
        "RT_PRICING_SOURCE",
        "BLOOMBERG_CLOSE_DT"
    ]
    
    for field in fields:
        request.append("fields", field)
        
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
                        ticker_returned = security.getElementAsString("security")
                        
                        print(f"\nTicker: {ticker_returned}")
                        print("-"*80)
                        
                        if security.hasElement("securityError"):
                            error = security.getElement("securityError")
                            print(f"ERROR: {error}")
                            continue
                            
                        if security.hasElement("fieldData"):
                            fieldData = security.getElement("fieldData")
                            
                            for field in fields:
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
                                        print(f"{field:25}: {value}")
                                        
        if event.eventType() == blpapi.Event.RESPONSE:
            break
            
    session.stop()
    print(f"\nToday's date: {date.today()}")

if __name__ == "__main__":
    # CSVから取得したティッカー
    ticker_from_csv = "LMCADS Q25U25<cmdty>"
    debug_single_spread(ticker_from_csv)