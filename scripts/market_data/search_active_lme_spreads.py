import blpapi
from datetime import datetime
import pandas as pd

class LMESpreadSearcher:
    def __init__(self):
        self.session = None
        self.instrument_service = None
        self.refdata_service = None
        
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
        
        return True
        
    def search_lme_copper_spreads(self):
        request = self.instrument_service.createRequest("instrumentListRequest")
        
        request.set("query", "LMCADS")
        request.set("yellowKeyFilter", "YK_FILTER_CMDT")
        request.set("maxResults", 1000)
        
        print("Searching for LME Copper spreads...")
        self.session.sendRequest(request)
        
        spreads = []
        
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
                                
                                if self._is_spread(security):
                                    description = result.getElementAsString("description") if result.hasElement("description") else ""
                                    spreads.append({
                                        "ticker": security,
                                        "description": description
                                    })
                                    
            if event.eventType() == blpapi.Event.RESPONSE:
                break
                
        return spreads
        
    def _is_spread(self, ticker):
        spread_patterns = [
            "LMCADS 03",
            "LMCADS M",
            "LMCADS F",
            "LMCADS G",
            "LMCADS H",
            "LMCADS J",
            "LMCADS K",
            "LMCADS N",
            "LMCADS Q",
            "LMCADS U",
            "LMCADS V",
            "LMCADS X",
            "LMCADS Z"
        ]
        
        return any(pattern in ticker for pattern in spread_patterns)
        
    def get_spread_activity(self, spreads):
        if not spreads:
            return []
            
        request = self.refdata_service.createRequest("ReferenceDataRequest")
        
        for spread in spreads:
            request.append("securities", spread["ticker"])
            
        request.append("fields", "PX_LAST")
        request.append("fields", "VOLUME")
        request.append("fields", "PX_VOLUME")
        request.append("fields", "LAST_UPDATE_DT")
        request.append("fields", "CONTRACT_VALUE")
        request.append("fields", "OPEN_INT")
        
        print(f"\nFetching activity data for {len(spreads)} spreads...")
        self.session.sendRequest(request)
        
        spread_data = {}
        
        while True:
            event = self.session.nextEvent()
            
            if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in event:
                    if msg.hasElement("securityData"):
                        securityData = msg.getElement("securityData")
                        
                        for i in range(securityData.numValues()):
                            security = securityData.getValueAsElement(i)
                            ticker = security.getElementAsString("security")
                            
                            if security.hasElement("fieldData"):
                                fieldData = security.getElement("fieldData")
                                
                                data = {
                                    "ticker": ticker,
                                    "px_last": self._get_field_value(fieldData, "PX_LAST"),
                                    "volume": self._get_field_value(fieldData, "VOLUME"),
                                    "px_volume": self._get_field_value(fieldData, "PX_VOLUME"),
                                    "last_update": self._get_field_value(fieldData, "LAST_UPDATE_DT"),
                                    "contract_value": self._get_field_value(fieldData, "CONTRACT_VALUE"),
                                    "open_interest": self._get_field_value(fieldData, "OPEN_INT")
                                }
                                
                                spread_data[ticker] = data
                                
            if event.eventType() == blpapi.Event.RESPONSE:
                break
                
        for spread in spreads:
            ticker = spread["ticker"]
            if ticker in spread_data:
                spread.update(spread_data[ticker])
                
        return spreads
        
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
        
    def filter_active_spreads(self, spreads, min_volume=0, min_open_interest=0):
        active_spreads = []
        
        for spread in spreads:
            volume = spread.get("volume", 0) or 0
            open_interest = spread.get("open_interest", 0) or 0
            
            if volume > min_volume or open_interest > min_open_interest:
                active_spreads.append(spread)
                
        return sorted(active_spreads, key=lambda x: (x.get("volume", 0) or 0), reverse=True)
        
    def stop_session(self):
        if self.session:
            self.session.stop()
            
    def display_results(self, spreads):
        if not spreads:
            print("No active spreads found.")
            return
            
        df = pd.DataFrame(spreads)
        
        print("\n" + "="*100)
        print("ACTIVE LME COPPER SPREADS")
        print("="*100)
        
        display_columns = ["ticker", "description", "px_last", "volume", "open_interest", "last_update"]
        available_columns = [col for col in display_columns if col in df.columns]
        
        df_display = df[available_columns].copy()
        
        if "volume" in df_display.columns:
            df_display["volume"] = df_display["volume"].fillna(0).astype(int)
        if "open_interest" in df_display.columns:
            df_display["open_interest"] = df_display["open_interest"].fillna(0).astype(int)
        if "px_last" in df_display.columns:
            df_display["px_last"] = df_display["px_last"].fillna(0).round(2)
            
        print(df_display.to_string(index=False))
        print(f"\nTotal active spreads found: {len(spreads)}")
        
        return df

def main():
    searcher = LMESpreadSearcher()
    
    try:
        if not searcher.start_session():
            print("Failed to start Bloomberg session. Make sure Bloomberg Terminal is running.")
            return
            
        spreads = searcher.search_lme_copper_spreads()
        print(f"Found {len(spreads)} potential spreads")
        
        spreads_with_data = searcher.get_spread_activity(spreads)
        
        active_spreads = searcher.filter_active_spreads(
            spreads_with_data,
            min_volume=0,
            min_open_interest=100
        )
        
        df = searcher.display_results(active_spreads)
        
        df.to_csv("active_lme_copper_spreads.csv", index=False)
        print("\nResults saved to: active_lme_copper_spreads.csv")
        
    finally:
        searcher.stop_session()

if __name__ == "__main__":
    main()