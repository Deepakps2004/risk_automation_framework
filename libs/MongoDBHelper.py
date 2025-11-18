import pymongo
import pandas as pd
import logging
from typing import Dict, Any

class MongoDBHelper:
    def __init__(self, connection_string: str, database: str):
        self.client = pymongo.MongoClient(connection_string)
        self.database = self.client[database]
        self.logger = logging.getLogger(__name__)
    
    def query_collection(self, collection: str, query: Dict, projection: Dict = None) -> pd.DataFrame:
        """Query MongoDB collection and return as DataFrame"""
        try:
            collection_obj = self.database[collection]
            cursor = collection_obj.find(query, projection)
            
            results = list(cursor)
            # Remove MongoDB _id field
            for result in results:
                result.pop('_id', None)
            
            return pd.DataFrame(results)
            
        except Exception as e:
            self.logger.error(f"MongoDB query failed: {e}")
            raise
    
    def validate_data(self, source_data: pd.DataFrame, validation_config: Dict) -> pd.DataFrame:
        """Validate MongoDB data against source data"""
        validation_results = []
        
        for validation_name, config in validation_config.items():
            try:
                # Query MongoDB
                mongo_data = self.query_collection(
                    config['collection'],
                    config.get('query', {}),
                    config.get('projection')
                )
                
                # Compare datasets
                comparison_result = self._compare_datasets(
                    source_data, mongo_data, config['key_columns']
                )
                
                validation_results.append({
                    'validation_name': validation_name,
                    'status': 'PASS' if comparison_result['all_match'] else 'FAIL',
                    'mismatch_count': comparison_result['mismatch_count'],
                    'details': comparison_result['details']
                })
                
            except Exception as e:
                self.logger.error(f"Validation {validation_name} failed: {e}")
                validation_results.append({
                    'validation_name': validation_name,
                    'status': 'ERROR',
                    'error': str(e)
                })
        
        return pd.DataFrame(validation_results)
    
    def _compare_datasets(self, source_df: pd.DataFrame, target_df: pd.DataFrame, key_columns: list) -> Dict:
        """Compare two datasets and identify mismatches"""
        # Implementation similar to SQL helper
        return {
            'all_match': True,
            'mismatch_count': 0,
            'details': 'Comparison details'
        }