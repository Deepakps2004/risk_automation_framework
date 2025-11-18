import pandas as pd
from pathlib import Path
import logging

class FileSystemHelper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def validate_files_exist(self, expected_files_path: str, target_directory: str) -> pd.DataFrame:
        """Validate that expected files exist in target directory"""
        try:
            # Read expected files from Excel
            expected_files_df = pd.read_excel(expected_files_path)
            target_path = Path(target_directory)
            
            results = []
            
            for _, row in expected_files_df.iterrows():
                expected_file = row['filename']  # Adjust column name as needed
                file_path = target_path / expected_file
                
                result = {
                    'filename': expected_file,
                    'expected_path': str(file_path),
                    'status': 'Found' if file_path.exists() else 'Missing',
                    'file_size': file_path.stat().st_size if file_path.exists() else 0
                }
                results.append(result)
            
            return pd.DataFrame(results)
            
        except Exception as e:
            self.logger.error(f"File validation failed: {e}")
            raise