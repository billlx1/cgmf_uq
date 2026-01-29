"""
cgmf_uq/workflow/indexing.py
Helper to validate manifests and index tasks
"""
from pathlib import Path
import csv

class TaskIndexer:
    def __init__(self, manifest_path: str):
        self.manifest_path = Path(manifest_path)
        self.tasks = []
        
    def validate_manifest(self) -> bool:
        """
        Check if manifest exists and all referenced config files exist.
        Returns True if valid.
        """
        if not self.manifest_path.exists():
            print(f"✗ Manifest missing: {self.manifest_path}")
            return False
            
        manifest_dir = self.manifest_path.parent
        valid = True
        
        try:
            with open(self.manifest_path, 'r') as f:
                reader = csv.DictReader(f)
                
                # Check for required columns
                required_cols = {'task_id', 'config_file'}
                if not required_cols.issubset(reader.fieldnames):
                    print(f"✗ Manifest missing required columns: {required_cols}")
                    print(f"  Found columns: {reader.fieldnames}")
                    return False
                
                for row in reader:
                    task_id = row['task_id']
                    config_file = row['config_file']
                    
                    # Handle both absolute and relative paths
                    config_path = Path(config_file)
                    if not config_path.is_absolute():
                        config_path = manifest_dir / config_file
                    
                    if not config_path.exists():
                        print(f"✗ Config missing for task {task_id}: {config_path}")
                        valid = False
                    else:
                        # Store the validated absolute path back in the row
                        row['config_path'] = str(config_path.resolve())
                    
                    self.tasks.append(row)
                    
        except Exception as e:
            print(f"✗ Error reading manifest: {e}")
            return False
        
        if valid and len(self.tasks) > 0:
            print(f"✓ {len(self.tasks)} configurations validated")
            
        return valid and len(self.tasks) > 0

    def get_total_tasks(self) -> int:
        return len(self.tasks)
    
    def get_task(self, task_id: int) -> dict:
        """
        Retrieve task information by ID
        
        Args:
            task_id: The task ID (0-indexed)
            
        Returns:
            Dictionary with task metadata including 'config_path'
        """
        if task_id < 0 or task_id >= len(self.tasks):
            raise IndexError(f"Task ID {task_id} out of range (0-{len(self.tasks)-1})")
        return self.tasks[task_id]
