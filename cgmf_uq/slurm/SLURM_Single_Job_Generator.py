"""
cgmf_uq/slurm/generator.py
Generate SLURM scripts from templates with variable substitution
"""
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import re


class SlurmScriptGenerator:
    """Generate SLURM job scripts from templates"""
    
    def __init__(self, template_path: Path):
        """
        Args:
            template_path: Path to bash template with {{VAR}} placeholders
        """
        self.template_path = Path(template_path)
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template not found: {template_path}")
        
        self.template_content = self.template_path.read_text()
        
        # Extract all placeholders for validation
        self.placeholders = set(re.findall(r'\{\{(\w+)\}\}', self.template_content))
    
    def generate(self, output_path: Path, variables: Dict[str, str]) -> Path:
        """
        Generate SLURM script by substituting variables in template
        
        Args:
            output_path: Where to write generated script
            variables: Dict of {PLACEHOLDER: value} to substitute
        
        Returns:
            Path to generated script
            
        Raises:
            ValueError: If required placeholders are missing or unsubstituted
        """
        # Add timestamp
        variables['TIMESTAMP'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Perform substitution
        content = self.template_content
        for key, value in variables.items():
            placeholder = f"{{{{{key}}}}}"  # {{KEY}}
            content = content.replace(placeholder, str(value))
        
        # Check for unsubstituted placeholders
        remaining = re.findall(r'\{\{(\w+)\}\}', content)
        if remaining:
            raise ValueError(
                f"Unsubstituted placeholders in template: {remaining}\n"
                f"Required: {self.placeholders}\n"
                f"Provided: {set(variables.keys())}"
            )
        
        # Write output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content)
        output_path.chmod(0o755)
        
        return output_path
    
    def validate_variables(self, variables: Dict[str, str]) -> tuple[bool, List[str]]:
        """
        Check if all required placeholders will be substituted
        
        Args:
            variables: Variables to be substituted
            
        Returns:
            (is_valid, list_of_missing_variables)
        """
        provided = set(variables.keys())
        provided.add('TIMESTAMP')  # Auto-added
        
        missing = self.placeholders - provided
        
        return len(missing) == 0, sorted(missing)
    
    def get_required_variables(self) -> List[str]:
        """Get list of all placeholders in template"""
        return sorted(self.placeholders)
    
    @staticmethod
    def validate_path_exists(variables: Dict[str, str], path_keys: List[str]) -> tuple[bool, List[str]]:
        """
        Validate that paths specified in variables actually exist
        
        Args:
            variables: Variable dictionary
            path_keys: Keys that should contain valid paths
            
        Returns:
            (all_exist, list_of_missing_paths)
        """
        missing = []
        for key in path_keys:
            if key in variables:
                path = Path(variables[key])
                if not path.exists():
                    missing.append(f"{key}={variables[key]}")
        
        return len(missing) == 0, missing


class ArrayJobConfig:
    """Helper to build variable dictionaries for array jobs"""
    
    def __init__(self, project_dir: Path, output_dir: Path):
        self.project_dir = Path(project_dir)
        self.output_dir = Path(output_dir)
    
    def build_sensitivity_vars(
        self,
        job_name: str,
        manifest_path: Path,
        total_tasks: int,
        max_concurrent: int,
        events: int,
        target_id: int,
        incident_e: float,
        cgmf_root: Path,
        cgmf_default_data: Path,
        post_processor: Path,
        conda_root: Path,
        conda_env: str,
        partition: str = "serial",
        time_limit: str = "04:00:00"
    ) -> Dict[str, str]:
        """
        Build complete variable dictionary for sensitivity runs
        
        Args:
            job_name: SLURM job name
            manifest_path: Path to manifest.csv
            total_tasks: Total number of array tasks
            max_concurrent: Max concurrent tasks
            events: CGMF events per task
            target_id: Nuclear target ID (e.g., 92235)
            incident_e: Incident neutron energy in eV
            cgmf_root: CGMF installation directory
            cgmf_default_data: Default CGMF data directory
            post_processor: Path to post-processing script
            conda_root: Conda installation root
            conda_env: Conda environment name
            partition: SLURM partition
            time_limit: Time limit per task (HH:MM:SS)
            
        Returns:
            Dictionary ready for SlurmScriptGenerator.generate()
        """
        return {
            # SLURM directives
            'JOB_NAME': job_name,
            'PARTITION': partition,
            'TIME_LIMIT': time_limit,
            'MAX_TASK_ID': str(total_tasks - 1),  # 0-indexed
            'MAX_CONCURRENT': str(max_concurrent),
            'LOG_DIR': str(self.output_dir / 'logs'),
            
            # Paths
            'PROJECT_DIR': str(self.project_dir),
            'MANIFEST': str(manifest_path),
            'OUTPUT_BASE': str(self.output_dir / 'runs'),
            'CGMF_ROOT': str(cgmf_root),
            'CGMF_DEFAULT_DATA': str(cgmf_default_data),
            'POST_PROCESSOR': str(post_processor),
            'CONDA_ROOT': str(conda_root),
            'CONDA_ENV': conda_env,
            
            # Run parameters
            'EVENTS': str(events),
            'TARGET_ID': str(target_id),
            'INCIDENT_E': str(incident_e),
        }
    
    def validate_paths(self, variables: Dict[str, str]) -> tuple[bool, List[str]]:
        """Validate critical paths exist before submission"""
        critical_paths = [
            'PROJECT_DIR',
            'CGMF_ROOT',
            'CGMF_DEFAULT_DATA',
            'POST_PROCESSOR',
            'CONDA_ROOT',
            'MANIFEST'
        ]
        
        return SlurmScriptGenerator.validate_path_exists(variables, critical_paths)
