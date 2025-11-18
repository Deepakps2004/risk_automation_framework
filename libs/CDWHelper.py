import requests
import pandas as pd
import xml.etree.ElementTree as ET
from typing import Dict, Any, List
import logging
import urllib3

# Disable SSL warnings for internal systems
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CDWHelper:
    def __init__(self, base_url: str, credentials: Dict[str, str]):
        self.base_url = base_url
        self.credentials = credentials
        self.session = requests.Session()
        self.session.verify = False  # Handle SSL errors
        self.logger = logging.getLogger(__name__)
        
        # Setup authentication
        self._setup_auth()
    
    def _setup_auth(self):
        """Setup authentication for CDW API"""
        # Implement your authentication logic here
        # This could be basic auth, token-based, etc.
        if 'username' in self.credentials and 'password' in self.credentials:
            self.session.auth = (
                self.credentials['username'], 
                self.credentials['password']
            )
    
    def flatten_xml(self, element: ET.Element, path: str = '', separator: str = '_') -> Dict[str, str]:
        """Recursively flatten XML structure"""
        flattened = {}
        
        # Handle element text
        if element.text and element.text.strip():
            key = path + separator + element.tag if path else element.tag
            flattened[key] = element.text.strip()
        
        # Handle attributes
        for attr, value in element.attrib.items():
            attr_key = f"{element.tag}{separator}{attr}"
            if path:
                attr_key = path + separator + attr_key
            flattened[attr_key] = value
        
        # Recursively process children
        for child in element:
            child_flattened = self.flatten_xml(child, 
                path + separator + element.tag if path else element.tag, 
                separator
            )
            flattened.update(child_flattened)
        
        return flattened
    
    def fetch_trade_data(self, trade_id: str, trade_date: str) -> Dict[str, Any]:
        """Fetch trade data from CDW API"""
        try:
            url = f"{self.base_url}/trade/{trade_id}?on={trade_date}"
            response = self.session.get(url)
            
            if response.status_code == 401:
                raise Exception("Authentication failed - check credentials")
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.content)
            
            # Flatten the entire XML structure
            flattened_data = self.flatten_xml(root)
            flattened_data['trade_id'] = trade_id
            flattened_data['trade_date'] = trade_date
            
            return flattened_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch trade {trade_id}: {e}")
            raise
    
    def extract_all_trades(self, trades_config: List[Dict]) -> pd.DataFrame:
        """Extract data for all trades and return as DataFrame"""
        all_trades_data = []
        
        for trade_config in trades_config:
            try:
                trade_data = self.fetch_trade_data(
                    trade_config['trade_id'],
                    trade_config['trade_date']
                )
                all_trades_data.append(trade_data)
                
            except Exception as e:
                self.logger.warning(f"Skipping trade {trade_config['trade_id']}: {e}")
                # Add error record
                error_record = {
                    'trade_id': trade_config['trade_id'],
                    'trade_date': trade_config['trade_date'],
                    'error': str(e)
                }
                all_trades_data.append(error_record)
        
        return pd.DataFrame(all_trades_data)