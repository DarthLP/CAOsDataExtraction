#!/usr/bin/env python3
"""
Sanity check script for the reorganized CAO data extraction repository.
Checks imports, syntax, configuration, and creates output directories.
"""

import os
import sys
import json
import subprocess
import importlib
from pathlib import Path
from datetime import datetime

def check_syntax():
    """Run compileall to check Python syntax."""
    print("Checking Python syntax...")
    try:
        result = subprocess.run([sys.executable, '-m', 'compileall', '.'], 
                              capture_output=True, text=True, cwd='.')
        if result.returncode == 0:
            print("✓ Syntax check passed")
            return True, result.stdout
        else:
            print("✗ Syntax check failed")
            print(result.stderr)
            return False, result.stderr
    except Exception as e:
        print(f"✗ Syntax check error: {e}")
        return False, str(e)

def check_pipeline_imports():
    """Check if pipeline modules can be imported."""
    print("\nChecking pipeline imports...")
    pipeline_modules = [
        'pipelines.p0_webscraping',
        'pipelines.p1_inputExcel',
        'pipelines.p2_extract',
        'pipelines.p3_llmExtraction',
        'pipelines.p3_1_llmExtraction',
        'pipelines.p4_analysis',
        'pipelines.p5_run'
    ]
    
    results = {}
    for module_name in pipeline_modules:
        try:
            module = importlib.import_module(module_name)
            if hasattr(module, 'main'):
                results[module_name] = {'status': 'success', 'has_main': True}
                print(f"✓ {module_name} - imported successfully, has main()")
            else:
                results[module_name] = {'status': 'success', 'has_main': False}
                print(f"✓ {module_name} - imported successfully, no main()")
        except Exception as e:
            results[module_name] = {'status': 'error', 'error': str(e)}
            print(f"✗ {module_name} - import failed: {e}")
    
    return results

def check_config():
    """Check configuration file and create missing directories."""
    print("\nChecking configuration...")
    try:
        import yaml
        with open('conf/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Check if all required paths exist
        required_paths = [
            'inputs_pdfs', 'inputs_excel', 'outputs_json', 'outputs_excel',
            'outputs_logs', 'outputs_analysis', 'outputs_comparison',
            'monitoring', 'utils', 'docs', 'scripts'
        ]
        
        missing_dirs = []
        for path_key in required_paths:
            if path_key in config['paths']:
                path = config['paths'][path_key]
                if not os.path.exists(path):
                    missing_dirs.append(path)
                    os.makedirs(path, exist_ok=True)
                    print(f"✓ Created missing directory: {path}")
                else:
                    print(f"✓ Directory exists: {path}")
            else:
                print(f"✗ Missing path key in config: {path_key}")
        
        return True, config, missing_dirs
    except Exception as e:
        print(f"✗ Configuration check failed: {e}")
        return False, None, []

def check_output_directories():
    """Create output directories for separate LLM extraction flows."""
    print("\nCreating output directories for separate flows...")
    try:
        import yaml
        with open('conf/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        json_output_dir = config['paths']['outputs_json']
        old_flow_dir = os.path.join(json_output_dir, 'old_flow')
        new_flow_dir = os.path.join(json_output_dir, 'new_flow')
        
        os.makedirs(old_flow_dir, exist_ok=True)
        os.makedirs(new_flow_dir, exist_ok=True)
        
        print(f"✓ Created old_flow directory: {old_flow_dir}")
        print(f"✓ Created new_flow directory: {new_flow_dir}")
        
        return True, [old_flow_dir, new_flow_dir]
    except Exception as e:
        print(f"✗ Failed to create flow directories: {e}")
        return False, []

def main():
    """Run all sanity checks."""
    print("=" * 60)
    print("CAO Data Extraction Repository - Sanity Check")
    print("=" * 60)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'syntax_check': {},
        'import_check': {},
        'config_check': {},
        'directory_check': {},
        'summary': {}
    }
    
    # Check syntax
    syntax_ok, syntax_output = check_syntax()
    results['syntax_check'] = {
        'passed': syntax_ok,
        'output': syntax_output
    }
    
    # Check imports
    import_results = check_pipeline_imports()
    results['import_check'] = import_results
    
    # Check configuration
    config_ok, config_data, missing_dirs = check_config()
    results['config_check'] = {
        'passed': config_ok,
        'missing_dirs_created': missing_dirs
    }
    
    # Check output directories
    dir_ok, flow_dirs = check_output_directories()
    results['directory_check'] = {
        'passed': dir_ok,
        'flow_dirs_created': flow_dirs
    }
    
    # Generate summary
    successful_imports = sum(1 for r in import_results.values() if r['status'] == 'success')
    total_imports = len(import_results)
    
    results['summary'] = {
        'syntax_passed': syntax_ok,
        'imports_passed': successful_imports,
        'total_imports': total_imports,
        'config_passed': config_ok,
        'directories_passed': dir_ok,
        'overall_status': 'PASS' if (syntax_ok and successful_imports == total_imports and config_ok and dir_ok) else 'FAIL'
    }
    
    # Save results
    with open('docs/reorg_sanity_check.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SANITY CHECK SUMMARY")
    print("=" * 60)
    print(f"Syntax check: {'✓ PASS' if syntax_ok else '✗ FAIL'}")
    print(f"Import check: {successful_imports}/{total_imports} modules imported successfully")
    print(f"Config check: {'✓ PASS' if config_ok else '✗ FAIL'}")
    print(f"Directory check: {'✓ PASS' if dir_ok else '✗ FAIL'}")
    print(f"Overall status: {results['summary']['overall_status']}")
    print(f"\nDetailed results saved to: docs/reorg_sanity_check.json")
    
    return results['summary']['overall_status'] == 'PASS'

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
