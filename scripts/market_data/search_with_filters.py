import blpapi
from datetime import datetime, timedelta
import re

class AdvancedLMESpreadSearcher:
    def __init__(self):
        self.session = None
        self.instrument_service = None
        self.refdata_service = None
        self.month_codes = {
            'F': 'Jan', 'G': 'Feb', 'H': 'Mar', 'J': 'Apr',
            'K': 'May', 'M': 'Jun', 'N': 'Jul', 'Q': 'Aug',
            'U': 'Sep', 'V': 'Oct', 'X': 'Nov', 'Z': 'Dec'
        }
        
    def start_session(self):
        options = blpapi.SessionOptions()
        options.setServerHost("localhost")
        options.setServerPort(8194)
        
        self.session = blpapi.Session(options)
        
        if not self.session.start():
            return False
            
        if not self.session.openService("//blp/instruments"):
            return False
            
        if not self.session.openService("//blp/refdata"):
            return False
            
        self.instrument_service = self.session.getService("//blp/instruments")
        self.refdata_service = self.session.getService("//blp/refdata")
        
        return True
        
    def search_spreads_by_pattern(self, patterns):
        all_spreads = []
        
        for pattern in patterns:
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
                                    description = result.getElementAsString("description") if result.hasElement("description") else ""
                                    
                                    spread_info = self.parse_spread_type(security, description)
                                    if spread_info:
                                        spread_info["ticker"] = security
                                        spread_info["description"] = description
                                        all_spreads.append(spread_info)
                                        
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
        return self.remove_duplicates(all_spreads)
        
    def parse_spread_type(self, ticker, description):
        if "LMCADS" not in ticker:
            return None
            
        spread_info = {
            "type": None,
            "near_month": None,
            "far_month": None,
            "near_year": None,
            "far_year": None
        }
        
        pattern_3m_3w = r'LMCADS 03([FGHJKMNQUVXZ])(\d{2})'
        match_3m_3w = re.search(pattern_3m_3w, ticker)
        if match_3m_3w:
            spread_info["type"] = "3M-3W"
            spread_info["far_month"] = match_3m_3w.group(1)
            spread_info["far_year"] = match_3m_3w.group(2)
            return spread_info
            
        pattern_calendar = r'LMCADS ([FGHJKMNQUVXZ])(\d{2})([FGHJKMNQUVXZ])(\d{2})'
        match_calendar = re.search(pattern_calendar, ticker)
        if match_calendar:
            spread_info["type"] = "Calendar"
            spread_info["near_month"] = match_calendar.group(1)
            spread_info["near_year"] = match_calendar.group(2)
            spread_info["far_month"] = match_calendar.group(3)
            spread_info["far_year"] = match_calendar.group(4)
            return spread_info
            
        return None
        
    def remove_duplicates(self, spreads):
        seen = set()
        unique = []
        for spread in spreads:
            ticker = spread["ticker"]
            if ticker not in seen:
                seen.add(ticker)
                unique.append(spread)
        return unique
        
    def get_activity_metrics(self, spreads):
        if not spreads:
            return []
            
        batch_size = 100
        all_results = []
        
        for i in range(0, len(spreads), batch_size):
            batch = spreads[i:i+batch_size]
            request = self.refdata_service.createRequest("ReferenceDataRequest")
            
            for spread in batch:
                request.append("securities", spread["ticker"])
                
            request.append("fields", "PX_LAST")
            request.append("fields", "VOLUME")
            request.append("fields", "PX_VOLUME")
            request.append("fields", "LAST_UPDATE_DT")
            request.append("fields", "OPEN_INT")
            request.append("fields", "PX_BID")
            request.append("fields", "PX_ASK")
            request.append("fields", "BID_ASK_SPREAD")
            request.append("fields", "TRADING_DT_REALTIME")
            
            self.session.sendRequest(request)
            
            batch_data = {}
            
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
                                        "px_last": self._get_field_value(fieldData, "PX_LAST"),
                                        "volume": self._get_field_value(fieldData, "VOLUME"),
                                        "px_volume": self._get_field_value(fieldData, "PX_VOLUME"),
                                        "last_update": self._get_field_value(fieldData, "LAST_UPDATE_DT"),
                                        "open_interest": self._get_field_value(fieldData, "OPEN_INT"),
                                        "bid": self._get_field_value(fieldData, "PX_BID"),
                                        "ask": self._get_field_value(fieldData, "PX_ASK"),
                                        "bid_ask_spread": self._get_field_value(fieldData, "BID_ASK_SPREAD"),
                                        "trading_date": self._get_field_value(fieldData, "TRADING_DT_REALTIME")
                                    }
                                    
                                    batch_data[ticker] = data
                                    
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
                    
            for spread in batch:
                ticker = spread["ticker"]
                if ticker in batch_data:
                    spread.update(batch_data[ticker])
                all_results.append(spread)
                
        return all_results
        
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
        
    def filter_active_spreads(self, spreads, days_since_trade=7, min_volume=0, min_open_interest=0, min_bid_ask_spread=None):
        active_spreads = []
        cutoff_date = datetime.now() - timedelta(days=days_since_trade)
        
        for spread in spreads:
            volume = spread.get("volume", 0) or 0
            open_interest = spread.get("open_interest", 0) or 0
            last_update = spread.get("last_update")
            bid_ask_spread = spread.get("bid_ask_spread")
            
            is_active = False
            
            if volume > min_volume:
                is_active = True
                
            if open_interest > min_open_interest:
                is_active = True
                
            if last_update:
                try:
                    update_date = datetime.strptime(last_update, "%Y-%m-%d")
                    if update_date >= cutoff_date:
                        is_active = True
                except:
                    pass
                    
            if min_bid_ask_spread is not None and bid_ask_spread is not None:
                if bid_ask_spread <= min_bid_ask_spread:
                    is_active = True
                    
            if is_active:
                active_spreads.append(spread)
                
        return sorted(active_spreads, key=lambda x: (x.get("volume", 0) or 0) + (x.get("open_interest", 0) or 0), reverse=True)
        
    def stop_session(self):
        if self.session:
            self.session.stop()

def main():
    searcher = AdvancedLMESpreadSearcher()
    
    try:
        if not searcher.start_session():
            print("Failed to start Bloomberg session")
            return
            
        print("Searching for LME Copper spreads...")
        
        search_patterns = [
            "LMCADS 03",
            "LMCADS F",
            "LMCADS G", 
            "LMCADS H",
            "LMCADS J",
            "LMCADS K",
            "LMCADS M",
            "LMCADS N",
            "LMCADS Q",
            "LMCADS U",
            "LMCADS V",
            "LMCADS X",
            "LMCADS Z"
        ]
        
        spreads = searcher.search_spreads_by_pattern(search_patterns)
        print(f"Found {len(spreads)} spreads")
        
        print("Fetching activity metrics...")
        spreads_with_metrics = searcher.get_activity_metrics(spreads)
        
        print("Filtering for active spreads...")
        active_spreads = searcher.filter_active_spreads(
            spreads_with_metrics,
            days_since_trade=30,
            min_volume=0,
            min_open_interest=100,
            min_bid_ask_spread=10
        )
        
        print(f"\nFound {len(active_spreads)} active spreads:")
        print("-" * 120)
        
        for spread in active_spreads[:20]:
            spread_type = spread.get("type", "Unknown")
            volume = spread.get("volume", 0) or 0
            oi = spread.get("open_interest", 0) or 0
            last_px = spread.get("px_last", 0) or 0
            bid = spread.get("bid", 0) or 0
            ask = spread.get("ask", 0) or 0
            
            print(f"{spread['ticker']:30} | Type: {spread_type:10} | Vol: {int(volume):6} | OI: {int(oi):6} | "
                  f"Last: {last_px:7.2f} | Bid/Ask: {bid:6.2f}/{ask:6.2f}")
                  
        if len(active_spreads) > 20:
            print(f"\n... and {len(active_spreads) - 20} more active spreads")
            
    finally:
        searcher.stop_session()

if __name__ == "__main__":
    main()