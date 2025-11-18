import pandas as pd
from pathlib import Path
from libs.FileSystemHelper import FileSystemHelper
import logging

def execute(config: dict, env: str) -> dict:
    logger = logging.getLogger(__name__)
    
    try:
        helper = FileSystemHelper()
        
        # Get environment-specific paths
        expected_files_path = config['paths'][env]['expected_files']
        target_directory = config['paths'][env]['target_directory']
        output_path = config['paths'][env]['output_report']
        
        # Validate files
        results_df = helper.validate_files_exist(expected_files_path, target_directory)
        
        # Save results
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results_df.to_excel(output_path, index=False)
        
        # Check if any files are missing
        missing_files = results_df[results_df['status'] == 'Missing']
        status = 'SUCCESS' if len(missing_files) == 0 else 'FAIL'
        
        return {
            'status': status,
            'output_file': output_path,
            'missing_files': len(missing_files),
            'total_files': len(results_df)
        }
        
    except Exception as e:
        logger.error(f"File validation task failed: {e}")
        return {
            'status': 'FAIL',
            'error': str(e)
        }