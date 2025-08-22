#!/usr/bin/env python3
"""
AST-based import rewrite script for the CAO data extraction repository reorganization.
Rewrites import statements to reflect the new file structure while preserving LLM prompts.
"""

import ast
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Move map: old module name -> new module name
MOVE_MAP = {
    # Pipeline files
    "0_webscrapping": "pipelines.p0_webscraping",
    "1_inputExcel": "pipelines.p1_inputExcel", 
    "2_extract": "pipelines.p2_extract",
    "3_llmExtraction": "pipelines.p3_llmExtraction",
    "3_1_llmExtraction": "pipelines.p3_1_llmExtraction",
    "4_analysis": "pipelines.p4_analysis",
    "5_run": "pipelines.p5_run",
    
    # Monitoring files
    "monitoring_3_1": "monitoring.monitoring_3_1",
    
    # Utility files
    "OUTPUT_analyze_empty_json_files": "utils.OUTPUT_analyze_empty_json_files",
    "OUTPUT_analyze_extracted_data": "utils.OUTPUT_analyze_extracted_data",
    "OUTPUT_compare_with_handAnalysis": "utils.OUTPUT_compare_with_handAnalysis",
    "OUTPUT_delete_cao_files": "utils.OUTPUT_delete_cao_files",
    "OUTPUT_merge_analysis_results": "utils.OUTPUT_merge_analysis_results",
    "OUTPUT_tracker": "utils.OUTPUT_tracker",
}

class ImportRewriter(ast.NodeTransformer):
    """AST transformer to rewrite import statements."""
    
    def __init__(self, move_map: Dict[str, str]):
        self.move_map = move_map
        self.changes_made = []
    
    def visit_Import(self, node: ast.Import) -> ast.Import:
        """Rewrite import statements."""
        for alias in node.names:
            old_name = alias.name
            if old_name in self.move_map:
                new_name = self.move_map[old_name]
                alias.name = new_name
                self.changes_made.append(f"Import: {old_name} -> {new_name}")
        return node
    
    def visit_ImportFrom(self, node: ast.ImportFrom) -> ast.ImportFrom:
        """Rewrite from ... import statements."""
        if node.module in self.move_map:
            old_module = node.module
            new_module = self.move_map[old_module]
            node.module = new_module
            self.changes_made.append(f"ImportFrom: {old_module} -> {new_module}")
        return node

def is_prompt_string(s: str) -> bool:
    """Check if a string looks like an LLM prompt (heuristic)."""
    prompt_indicators = ["prompt", "PROMPT", "system", "instruction", "template", "gemini", "chat"]
    s_lower = s.lower()
    return any(indicator in s_lower for indicator in prompt_indicators)

def rewrite_file(file_path: Path, move_map: Dict[str, str]) -> Tuple[bool, List[str]]:
    """Rewrite imports in a single file using AST."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse the file
        tree = ast.parse(content)
        
        # Create rewriter and apply transformations
        rewriter = ImportRewriter(move_map)
        new_tree = rewriter.visit(tree)
        
        if rewriter.changes_made:
            # Generate new code
            import astor
            new_content = astor.to_source(new_tree)
            
            # Write backup
            backup_path = Path("scripts/backups") / f"{file_path.name}.import_rewrite"
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            # Write updated file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            return True, rewriter.changes_made
        
        return False, []
        
    except Exception as e:
        return False, [f"Error processing {file_path}: {str(e)}"]

def main():
    """Main function to rewrite imports across the repository."""
    print("Starting AST-based import rewrite...")
    
    # Find all Python files
    python_files = []
    for root, dirs, files in os.walk('.'):
        # Skip git, venv, and other directories
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['venv', 'env', 'node_modules']]
        
        for file in files:
            if file.endswith('.py'):
                python_files.append(Path(root) / file)
    
    print(f"Found {len(python_files)} Python files to process")
    
    # Process each file
    total_changes = 0
    all_changes = []
    
    for file_path in python_files:
        print(f"Processing: {file_path}")
        changed, changes = rewrite_file(file_path, MOVE_MAP)
        
        if changed:
            total_changes += 1
            all_changes.extend([f"{file_path}: {change}" for change in changes])
            print(f"  Updated {len(changes)} imports")
        else:
            print(f"  No changes needed")
    
    # Write summary
    summary = {
        "files_processed": len(python_files),
        "files_changed": total_changes,
        "total_imports_updated": len(all_changes),
        "changes": all_changes
    }
    
    with open("docs/import_rewrite_summary.json", 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nImport rewrite completed:")
    print(f"  Files processed: {len(python_files)}")
    print(f"  Files changed: {total_changes}")
    print(f"  Total imports updated: {len(all_changes)}")
    print(f"  Summary written to: docs/import_rewrite_summary.json")

if __name__ == "__main__":
    main()
