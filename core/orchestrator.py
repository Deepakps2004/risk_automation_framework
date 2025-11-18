import json
import logging
from typing import Dict, Any
from pathlib import Path
from .task_registry import TaskRegistry

class Orchestrator:
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.task_registry = TaskRegistry()
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def load_suite_config(self, suite_path: str) -> Dict[str, Any]:
        """Load and validate suite configuration"""
        try:
            with open(suite_path, 'r') as f:
                suite_config = json.load(f)
            
            required_keys = ['suite_name', 'tasks']
            if not all(key in suite_config for key in required_keys):
                raise ValueError(f"Suite config missing required keys: {required_keys}")
                
            return suite_config
        except Exception as e:
            self.logger.error(f"Failed to load suite config: {e}")
            raise
    
    def execute_suite(self, suite_path: str, env: str = "UAT") -> Dict[str, Any]:
        """Execute a test suite"""
        suite_config = self.load_suite_config(suite_path)
        self.logger.info(f"Executing suite: {suite_config['suite_name']}")
        
        results = {
            'suite_name': suite_config['suite_name'],
            'environment': env,
            'tasks': []
        }
        
        for task_config in suite_config['tasks']:
            task_name = task_config['task_name']
            task_config_path = self.config_dir / 'tasks' / task_config['config_file']
            
            try:
                self.logger.info(f"Executing task: {task_name}")
                
                # Load task-specific configuration
                with open(task_config_path, 'r') as f:
                    task_params = json.load(f)
                
                # Execute task
                task_result = self.task_registry.execute_task(
                    task_name, 
                    task_params, 
                    env
                )
                
                results['tasks'].append({
                    'task_name': task_name,
                    'status': task_result['status'],
                    'output_file': task_result.get('output_file'),
                    'error': task_result.get('error')
                })
                
                self.logger.info(f"Task {task_name} completed with status: {task_result['status']}")
                
            except Exception as e:
                self.logger.error(f"Task {task_name} failed: {e}")
                results['tasks'].append({
                    'task_name': task_name,
                    'status': 'FAIL',
                    'error': str(e)
                })
        
        return results