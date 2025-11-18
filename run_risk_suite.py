#!/usr/bin/env python3
import argparse
import sys
import json
from pathlib import Path
from core.orchestrator import Orchestrator

def main():
    parser = argparse.ArgumentParser(description='RISK Validation Automation Framework')
    parser.add_argument('--env', required=True, choices=['UAT', 'PROD'], 
                       help='Environment to run tests against')
    parser.add_argument('--suite', required=True, 
                       help='Path to the suite configuration file')
    parser.add_argument('--config-dir', default='config',
                       help='Base configuration directory')
    
    args = parser.parse_args()
    
    # Validate suite file exists
    if not Path(args.suite).exists():
        print(f"Error: Suite file not found: {args.suite}")
        sys.exit(1)
    
    try:
        # Initialize and run orchestrator
        orchestrator = Orchestrator(args.config_dir)
        results = orchestrator.execute_suite(args.suite, args.env)
        
        # Print summary
        print(f"\n=== Suite Execution Summary ===")
        print(f"Suite: {results['suite_name']}")
        print(f"Environment: {results['environment']}")
        print(f"Total Tasks: {len(results['tasks'])}")
        
        successful_tasks = [t for t in results['tasks'] if t['status'] == 'SUCCESS']
        print(f"Successful: {len(successful_tasks)}")
        print(f"Failed: {len(results['tasks']) - len(successful_tasks)}")
        
        # Print individual task results
        print(f"\n--- Task Details ---")
        for task in results['tasks']:
            status_icon = "✅" if task['status'] == 'SUCCESS' else "❌"
            print(f"{status_icon} {task['task_name']}: {task['status']}")
            if task.get('error'):
                print(f"   Error: {task['error']}")
        
        # Exit with appropriate code
        if len(successful_tasks) == len(results['tasks']):
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"Framework execution failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()