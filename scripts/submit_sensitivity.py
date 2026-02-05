#!/usr/bin/env python3
"""
scripts/submit_sensitivity.py
Orchestrate sensitivity study: generate configs, populate template, submit array job
"""
import argparse
import subprocess
from pathlib import Path
import sys
import re

# Add project to path to find local modules
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import local modules (Ensure these files exist in cgmf_uq/)
from cgmf_uq.workflow.indexing import TaskIndexer
from cgmf_uq.slurm.SLURM_Single_Job_Generator import SlurmScriptGenerator

def generate_configurations(
    registry_path: Path,
    sensitivity_path: Path,
    output_dir: Path,
    project_dir: Path
) -> tuple[Path, Path, int]:
    """
    Step 1: Call the JSON generator to create perturbed input files.
    """
    print("[1/5] Generating parameter configurations...")
    
    config_dir = output_dir / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    
    json_generator_script = project_dir / "cgmf_uq/io/generate_scale_factor_json.py"
    
    if not json_generator_script.exists():
        print(f"ERROR: JSON generator script not found at {json_generator_script}")
        sys.exit(1)

    result = subprocess.run([
        "python",
        str(json_generator_script),
        "--registry", str(registry_path),
        "--sensitivity", str(sensitivity_path),
        "--output", str(config_dir),
        "--force"
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print("ERROR: Configuration generation failed")
        print("STDERR:", result.stderr)
        sys.exit(1)
    
    print(result.stdout)
    
    manifest_path = config_dir / "manifest.txt"
    job_map_path = config_dir / "job_map.txt"
    
    # Verify outputs exist
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not created: {manifest_path}")
    if not job_map_path.exists():
        raise FileNotFoundError(f"Job map not created: {job_map_path}")
    
    # Don't count manually - let validate_configurations do it via TaskIndexer
    # This ensures consistency between what we say we have and what validator sees
    return manifest_path, job_map_path, 0  # Return 0 as placeholder

def validate_configurations(manifest_path: Path) -> int:
    """
    Step 2: Ensure every task in the manifest points to a real JSON file.
    Returns the actual task count.
    """
    print("\n[2/5] Validating configurations...")
    
    indexer = TaskIndexer(str(manifest_path))
    
    if not indexer.validate_manifest():
        print("ERROR: Manifest validation failed. See errors above.")
        sys.exit(1)
    
    total = indexer.get_total_tasks()
    return total
    

def calculate_resources(total_tasks: int, max_concurrent: int, events: int, time_limit: str) -> None:
    """
    Step 3: Print a summary of what we are about to request from SLURM.
    """
    print("\n[3/5] Resource allocation:")
    print(f"  Total tasks:       {total_tasks}")
    print(f"  Max concurrent:    {max_concurrent} cores")
    print(f"  Events per task:   {events:,}")
    print(f"  Time per task:     {time_limit}")
    
    # Estimate batches
    batches = (total_tasks + max_concurrent - 1) // max_concurrent
    print(f"  Sequential batches: ~{batches}")

def generate_slurm_script(
    template_path: Path,
    output_dir: Path,
    manifest_path: Path,
    job_map_path: Path,
    total_tasks: int,
    config: dict
) -> Path:
    """
    Step 4: Use the Template + Generator to write the .sbatch file.
    """
    print("\n[4/5] Generating SLURM job script...")
    
    generator = SlurmScriptGenerator(template_path)
    
    # Map Python variables to the {{PLACEHOLDERS}} in the Bash template
    variables = {
        # SLURM directives
        'JOB_NAME': config['job_name'],
        'PARTITION': config['partition'],
        'TIME_LIMIT': config['time_limit'],
        'MAX_TASK_ID': str(total_tasks - 1),  # SLURM arrays are 0-indexed
        'MAX_CONCURRENT': str(config['max_concurrent']),
        'LOG_DIR': str(output_dir / 'logs'),
        
        # Paths
        'PROJECT_DIR': str(config['project_dir']),
        'JOB_MAP': str(job_map_path),
        'MANIFEST': str(manifest_path),
        'OUTPUT_BASE': str(output_dir / 'runs'),
        'CGMF_ROOT': str(config['cgmf_root']),
        'CGMF_DEFAULT_DATA': str(config['cgmf_default_data']),
        'POST_PROCESSOR': str(config['post_processor']),
        'CONDA_ROOT': str(config['conda_root']),
        'CONDA_ENV': config['conda_env'],
        
        # Run parameters
        'EVENTS': str(config['events']),
        'TARGET_ID': str(config['target_id']),
        'INCIDENT_E': str(config['incident_e']),
    }
    
    # Generate script
    script_path = output_dir / f"{config['job_name']}.sbatch"
    generator.generate(script_path, variables)
    
    print(f"✓ SLURM script generated: {script_path}")
    return script_path

def submit_job(script_path: Path, dry_run: bool) -> None:
    """
    Step 5: Run 'sbatch' to submit the job.
    """
    print("\n[5/5] Submission:")
    
    if dry_run:
        print("DRY RUN - Script generated but not submitted")
        print(f"\nTo submit manually:")
        print(f"  sbatch {script_path}")
    else:
        print("Submitting to SLURM...")
        
        result = subprocess.run(
            ["sbatch", str(script_path)],
            capture_output=True,
            text=True,
            cwd=script_path.parent
        )
        
        if result.returncode == 0:
            print(result.stdout.strip())
            print("✓ Job submitted successfully")
            
            # Extract job ID for monitoring
            match = re.search(r'Submitted batch job (\d+)', result.stdout)
            if match:
                job_id = match.group(1)
                print(f"\nMonitoring commands:")
                print(f"  squeue -j {job_id}")
                print(f"  tail -f {script_path.parent}/logs/*_{job_id}_0.out")
        else:
            print("ERROR: Submission failed")
            print(result.stderr)
            sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Generate and submit CGMF sensitivity study as SLURM array job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # --- REQUIRED ARGUMENTS ---
    parser.add_argument("--registry", required=True, help="Path to Parameter_Registry.yaml")
    parser.add_argument("--sensitivity", required=True, help="Path to Sensitivity_Coeff.yaml")
    parser.add_argument("--output", required=True, help="Directory where results/logs will be saved")
    
    # --- HPC PATHS (Defaults set to your cluster env) ---
    parser.add_argument("--project-dir", 
                        default=str(PROJECT_ROOT),
                        help="Root directory of the project")
                        
    parser.add_argument("--cgmf-root",
                        default="/mnt/iusers01/fatpou01/phy01/mbcxawh2/software/CGMF_MY_VERSION/CGMF_GDR_Params",
                        help="Location of CGMF installation")
                        
    parser.add_argument("--conda-root",
                        default="/mnt/iusers01/fatpou01/phy01/mbcxawh2/miniforge3",
                        help="Root of Conda install (containing etc/profile.d/conda.sh)")
                        
    parser.add_argument("--cgmf-default-data",
                        default="/mnt/iusers01/fatpou01/phy01/mbcxawh2/software/PROJECT_ROOT/CGMF_Data_Default",
                        help="Path to clean/unperturbed CGMF data")
                        
    parser.add_argument("--post-processor",
                        default="/mnt/iusers01/fatpou01/phy01/mbcxawh2/software/PROJECT_ROOT/scripts/post_processing.py",
                        help="Path to analysis Python script")
    
    # --- RUN PARAMETERS ---
    parser.add_argument("--events", type=int, default=5000, help="CGMF events per task")
    parser.add_argument("--target-id", type=int, default=92235, help="Target ZAID (e.g. 92235)")
    parser.add_argument("--incident-e", type=float, default=0.0, help="Incident energy (eV)")
    parser.add_argument("--job-name", default="cgmf_sens", help="Name in SLURM queue")
    parser.add_argument("--conda-env", default="cgmf_py", help="Conda environment name")
    
    # --- SLURM RESOURCES ---
    parser.add_argument("--max-concurrent", type=int, default=50, help="Max simultaneous tasks")
    parser.add_argument("--partition", default="serial", help="Queue partition name")
    parser.add_argument("--time-limit", default="04:00:00", help="Time limit (HH:MM:SS)")
    
    # --- FLAGS ---
    parser.add_argument("--submit", action="store_true", help="Actually submit the job (default is dry-run)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output directory")
    
    args = parser.parse_args()
    
    # --- SETUP & EXECUTION ---
    project_dir = Path(args.project_dir)
    output_dir = Path(args.output)
    
    # Check output safety
    if output_dir.exists() and not args.force:
        print(f"ERROR: Output directory exists: {output_dir}")
        print("Use --force to overwrite")
        sys.exit(1)
        
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "logs").mkdir(exist_ok=True)
    (output_dir / "runs").mkdir(exist_ok=True)
    
    template_path = project_dir / "cgmf_uq" / "slurm" / "sensitivity_job_template.sh"
    if not template_path.exists():
        print(f"ERROR: Template not found at {template_path}")
        sys.exit(1)

    print("=" * 60)
    print(f"CGMF ORCHESTRATOR | {args.job_name}")
    print("=" * 60)

    # 1. Generate Configs
    manifest_path, job_map_path, _ = generate_configurations(
        Path(args.registry), Path(args.sensitivity), output_dir, project_dir
    )

    # Validate returns the actual count
    total_tasks = validate_configurations(manifest_path)
    
    # 2. Validate
    validate_configurations(manifest_path)
    
    # 3. Resources
    calculate_resources(total_tasks, args.max_concurrent, args.events, args.time_limit)
    
    # 4. Generate Script
    config_dict = {
        'job_name': args.job_name,
        'partition': args.partition,
        'time_limit': args.time_limit,
        'max_concurrent': args.max_concurrent,
        'project_dir': project_dir,
        'cgmf_root': args.cgmf_root,
        'cgmf_default_data': args.cgmf_default_data,
        'post_processor': args.post_processor,
        'conda_root': args.conda_root,
        'conda_env': args.conda_env,
        'events': args.events,
        'target_id': args.target_id,
        'incident_e': args.incident_e
    }
    
    script_path = generate_slurm_script(
        template_path, output_dir, manifest_path, job_map_path, total_tasks, config_dict
    )
    
    # 5. Submit
    submit_job(script_path, not args.submit)

if __name__ == "__main__":
    main()
