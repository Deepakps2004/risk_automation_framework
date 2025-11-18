import pandas as pd
from pathlib import Path
from libs.CDWHelper import CDWHelper
import logging

def execute(config: dict, env: str) -> dict:
    logger = logging.getLogger(__name__)
    
    try:
        # Get environment-specific configuration
        env_config = config['environments'][env]
        
        # Initialize CDW helper
        helper = CDWHelper(
            base_url=env_config['base_url'],
            credentials=env_config['credentials']
        )
        
        # Load trade list
        trades_df = pd.read_excel(env_config['trade_list_path'])
        trades_config = trades_df.to_dict('records')
        
        # Extract all trades
        results_df = helper.extract_all_trades(trades_config)
        
        # Save results
        output_path = env_config['output_path']
        output_dir = Path(output_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results_df.to_excel(output_path, index=False)
        
        # Check for errors
        error_trades = results_df[results_df.get('error', '').notna()]
        status = 'SUCCESS' if len(error_trades) == 0 else 'PARTIAL'
        
        return {
            'status': status,
            'output_file': output_path,
            'total_trades': len(results_df),
            'failed_trades': len(error_trades)
        }
        
    except Exception as e:
        logger.error(f"CDW extraction task failed: {e}")
        return {
            'status': 'FAIL',
            'error': str(e)
        }