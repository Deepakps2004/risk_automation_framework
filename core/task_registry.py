import importlib
from typing import Dict, Any, Callable

class TaskRegistry:
    def __init__(self):
        self.tasks: Dict[str, Callable] = {}
        self._register_tasks()
    
    def _register_tasks(self):
        """Register all available tasks"""
        task_mappings = {
            'files_in_folder_task': 'tasks.files_in_folder_task.execute',
            'cdw_extraction_task': 'tasks.cdw_extraction_task.execute',
            'sql_validation_task': 'tasks.sql_validation_task.execute',
            'mongo_validation_task': 'tasks.mongo_validation_task.execute'
        }
        
        for task_name, import_path in task_mappings.items():
            try:
                module_path, function_name = import_path.rsplit('.', 1)
                module = importlib.import_module(module_path)
                task_function = getattr(module, function_name)
                self.tasks[task_name] = task_function
            except (ImportError, AttributeError) as e:
                raise ImportError(f"Failed to register task {task_name}: {e}")
    
    def execute_task(self, task_name: str, config: Dict[str, Any], env: str) -> Dict[str, Any]:
        """Execute a registered task"""
        if task_name not in self.tasks:
            raise ValueError(f"Task not registered: {task_name}")
        
        task_function = self.tasks[task_name]
        return task_function(config, env)