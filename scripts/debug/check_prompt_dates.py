import blpapi
from datetime import datetime, date

def check_prompt_dates():
    """LMCADS03とLMCADS00のLME_PROMPT_DTを確認"""
    
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
    
    # テスト用のティッカー
    test_tickers = [
        "LMCADS03 Comdty",  # 3ヶ月先物
        "LMCADS00 Comdty",  # Cash
        "LMCADS03 COMDTY",  # 大文字でも試す
        "LMCADS00 COMDTY"
    ]
    
    print(f"Checking prompt dates for LME Copper 3M and Cash")
    print(f"Today's date: {date.today()}")
    print("="*80)
    
    for ticker in test_tickers:
        request.append("securities", ticker)
        
    # フィールド
    fields = [
        "LME_PROMPT_DT",
        "FUT_NOTICE_FIRST",
        "LAST_TRADEABLE_DT",
        "FUT_DLV_DT_FIRST",
        "FUT_DLV_DT_LAST",
        "MATURITY",
        "SETTLE_DT",
        "FUT_CONT_SIZE",
        "LAST_UPDATE_DT",
        "PX_LAST"
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

if __name__ == "__main__":
    check_prompt_dates()