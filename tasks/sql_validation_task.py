import pandas as pd
from pathlib import Path
from libs.SQLServerHelper import SQLServerHelper
import logging

def execute(config: dict, env: str) -> dict:
    logger = logging.getLogger(__name__)
    
    try:
        env_config = config['environments'][env]
        
        # Initialize SQL helper
        helper = SQLServerHelper(env_config['connection_string'])
        
        # Load source data (from CDW extraction)
        source_data_path = env_config['source_data_path']
        source_df = pd.read_excel(source_data_path)
        
        # Perform validations
        validation_results = helper.validate_data(
            source_df, 
            env_config['validation_queries']
        )
        
        # Save results
        output_path = env_config['output_path']
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        validation_results.to_excel(output_path, index=False)
        
        # Determine overall status
        failed_validations = validation_results[validation_results['status'] == 'FAIL']
        status = 'SUCCESS' if len(failed_validations) == 0 else 'FAIL'
        
        return {
            'status': status,
            'output_file': output_path,
            'total_validations': len(validation_results),
            'failed_validations': len(failed_validations)
        }
        
    except Exception as e:
        logger.error(f"SQL validation task failed: {e}")
        return {
            'status': 'FAIL',
            'error': str(e)
        }