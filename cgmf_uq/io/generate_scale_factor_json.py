#!/usr/bin/env python3
"""
Phase I: Sensitivity Input Generator
====================================

This script mimics the "Submit" phase of your HPC workflow.
1. It reads the Sensitivity Config (which parameters to vary).
2. It reads the Registry (how to map parameter names to JSON).
3. It generates a directory of physical JSON files.
4. It generates a 'manifest.txt' for SLURM to iterate over.
5. It generates a 'job_map.txt' for simple SLURM array indexing.

Output Structure:
    inputs/
    └── sensitivity_sweep/
        ├── manifest.txt          # Full metadata: task_id, parameter, scale, config_file
        ├── job_map.txt           # Simple list: one config path per line
        ├── global_PSF_norm_0.900.json
        ├── global_PSF_norm_1.100.json
        └── ...

Usage Examples:
    # Use default paths
    ./generate_sensitivity_inputs.py
    
    # Custom paths
    ./generate_sensitivity_inputs.py --registry my_registry.yaml --output results/
    
    # Overwrite existing output
    ./generate_sensitivity_inputs.py --force
"""

import sys
import yaml
import json
import argparse
from pathlib import Path
import shutil
from collections import defaultdict

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

try:
    from cgmf_uq.io.param_json_yaml_mapper import ParameterMapper
except ImportError:
    print("ERROR: Could not import ParameterMapper.")
    print("Ensure you are running this from the project root and 'cgmf_uq' is a valid package.")
    sys.exit(1)


def parse_args():
    """
    Parse command-line arguments for the sensitivity sweep generator.
    
    Arguments:
        --registry: Path to the Parameter Registry YAML file that defines the mapping
                   between high-level parameter names and their JSON structure.
                   This file tells the script how to construct the full 55-parameter
                   JSON from a single parameter perturbation.
                   Default: YAML/Parameter_Registry.yaml
        
        --sensitivity: Path to the Sensitivity Coefficients YAML file that specifies:
                      - Which parameters to vary (enabled: true/false)
                      - What scaling factors to apply to each parameter
                      Example: global_PSF_norm with scaling_factors: [0.9, 1.0, 1.1]
                      Default: YAML/Sensitivity_Coeff.yaml
        
        --output: Output directory where generated JSON files and manifest will be written.
                 The script will create this directory if it doesn't exist.
                 WARNING: Use --force if directory exists and you want to overwrite.
                 Default: MISC/TEST_JSON_GEN
        
        --force: Allow overwriting of existing output directory without prompting.
                This prevents accidental deletion of previous sensitivity sweep results.
                Use with caution in production workflows.
                Default: False (script will exit if output exists)
    
    Returns:
        argparse.Namespace: Parsed arguments with Path objects for file paths
    """
    parser = argparse.ArgumentParser(
        description="Generate sensitivity sweep input files for HPC runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --registry custom_registry.yaml --output my_sweep/
  %(prog)s --force --output existing_directory/
        """
    )
    
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("YAML/Parameter_Registry.yaml"),
        help="Path to Parameter Registry YAML (default: %(default)s)"
    )
    
    parser.add_argument(
        "--sensitivity",
        type=Path,
        default=Path("YAML/Sensitivity_Coeff.yaml"),
        help="Path to Sensitivity Coefficients YAML (default: %(default)s)"
    )
    
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("MISC/TEST_JSON_GEN"),
        help="Output directory for generated files (default: %(default)s)"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output directory without prompting"
    )
    
    return parser.parse_args()


def validate_inputs(registry_path, sens_path):
    """
    Validate that required input files exist.
    
    Args:
        registry_path: Path to the parameter registry file
        sens_path: Path to the sensitivity configuration file
    
    Exits:
        Terminates the script if any required file is missing
    """
    if not registry_path.exists():
        print(f"ERROR: Registry not found: {registry_path}")
        print(f"       Expected absolute path: {registry_path.resolve()}")
        sys.exit(1)
    
    if not sens_path.exists():
        print(f"ERROR: Sensitivity config not found: {sens_path}")
        print(f"       Expected absolute path: {sens_path.resolve()}")
        sys.exit(1)


def write_job_map(output_dir, json_files_list):
    """
    Writes a simple text file containing the absolute path 
    to one JSON configuration per line.
    
    This provides a simple interface for SLURM array jobs:
    CONFIG_PATH=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" job_map.txt)
    
    Args:
        output_dir: Directory where job_map.txt will be written
        json_files_list: List of Path objects pointing to JSON config files
    
    Returns:
        Path to the created job_map.txt file
    """
    # Sort to ensure reproducibility (Job 0 always = same file)
    json_files_list.sort(key=lambda x: x.name)
    
    job_map_path = output_dir / "job_map.txt"
    with open(job_map_path, "w") as f:
        for json_file in json_files_list:
            f.write(f"{json_file.resolve()}\n")
    
    return job_map_path


def generate_sweep(args):
    """
    Main generation function for sensitivity sweep inputs.
    
    Args:
        args: Parsed command-line arguments containing paths and options
    
    Process:
        1. Validates input files exist
        2. Sets up output directory (with --force protection)
        3. Loads parameter mapper and sensitivity configuration
        4. Iterates through enabled parameters and scaling factors
        5. Generates JSON files for each combination
        6. Creates manifest file for SLURM array job indexing
        7. Creates job_map file for simple bash array access
        8. Prints summary statistics
    """
    # --- Configuration from Arguments ---
    registry_path = args.registry
    sens_path = args.sensitivity
    output_dir = args.output
    manifest_path = output_dir / "manifest.txt"

    # --- Validation ---
    print(f"--- Starting Phase I Generation ---")
    print(f"Registry:     {registry_path.resolve()}")
    print(f"Sensitivity:  {sens_path.resolve()}")
    print(f"Output:       {output_dir.resolve()}\n")
    
    validate_inputs(registry_path, sens_path)
    
    # --- Setup Output Directory ---
    if output_dir.exists():
        if not args.force:
            print(f"ERROR: Output directory exists: {output_dir}")
            print(f"       Use --force to overwrite existing files")
            sys.exit(1)
        print(f"WARNING: Overwriting existing directory: {output_dir}")
        shutil.rmtree(output_dir)
    
    output_dir.mkdir(parents=True)
    print(f"Created output directory: {output_dir}\n")

    # --- Initialize Parameter Mapper ---
    try:
        mapper = ParameterMapper(registry_path)
    except Exception as e:
        print(f"CRITICAL: Registry Error: {e}")
        sys.exit(1)

    # --- Load Sensitivity Configuration ---
    with open(sens_path) as f:
        sens_config = yaml.safe_load(f)

    # --- Generation Loop ---
    manifest_lines = []
    json_file_paths = []
    job_id_counter = 0  # Corresponds to SLURM_ARRAY_TASK_ID
    
    # Track statistics for summary
    param_stats = defaultdict(int)  # parameter -> count of configurations
    enabled_params = []
    disabled_params = []
    skipped_params = []
    
    # Iterate over parameters in Sensitivity_Coeff.yaml
    # Note: Adapting to structure where parameters are under root or 'parameters' key
    studies = sens_config.get('parameters', sens_config)

    print(f"{'ID':<4} | {'Parameter':<30} | {'Scale':<6} | {'Filename'}")
    print("-" * 80)

    for param_name, config in studies.items():
        # Check if parameter is enabled for this sweep
        if not config.get('enabled', False):
            disabled_params.append(param_name)
            continue
        
        enabled_params.append(param_name)
        scaling_factors = config['scaling_factors']
        
        for scale in scaling_factors:
            # 1. Generate the JSON content
            try:
                # This creates the full 55-parameter structure with 1 perturbation
                perturbation = {param_name: scale}
                json_content = mapper.registry_to_json_structure(perturbation)
            except ValueError as e:
                print(f"WARNING: Skipping {param_name}@{scale}: {e}")
                skipped_params.append(f"{param_name}@{scale}")
                continue

            # 2. Define Filename
            # Format: param_name_scale.json (e.g., global_PSF_norm_0.950.json)
            filename = f"{param_name}_{scale:.3f}.json"
            file_path = output_dir / filename

            # 3. Write JSON to disk
            with open(file_path, 'w') as f:
                json.dump(json_content, f, indent=2)
            
            # Track for job_map generation
            json_file_paths.append(file_path)

            # 4. Add to Manifest Record
            # Format: TaskID, ParamName, Scale, Filename (relative to manifest dir)
            # Note: Using 'task_id' and 'config_file' to match TaskIndexer expectations
            manifest_entry = f"{job_id_counter},{param_name},{scale},{filename}"
            manifest_lines.append(manifest_entry)
            
            # 5. Update statistics
            param_stats[param_name] += 1
            
            print(f"{job_id_counter:<4} | {param_name:<30} | {scale:<6.3f} | {filename}")
            
            job_id_counter += 1

    # --- Write Manifest ---
    with open(manifest_path, 'w') as f:
        # Header matches TaskIndexer expectations: task_id, config_file
        # Plus additional metadata: parameter, scale
        f.write("task_id,parameter,scale,config_file\n")
        f.write("\n".join(manifest_lines))

    # --- Write Job Map ---
    job_map_path = write_job_map(output_dir, json_file_paths)

    # --- Print Summary ---
    print("-" * 80)
    print(f"\n{'='*80}")
    print(f"{'GENERATION SUMMARY':^80}")
    print(f"{'='*80}\n")
    
    print(f"Total Configurations Generated: {job_id_counter}")
    print(f"  • Parameters Enabled:  {len(enabled_params)}")
    print(f"  • Parameters Disabled: {len(disabled_params)}")
    if skipped_params:
        print(f"  • Configurations Skipped: {len(skipped_params)} (due to errors)")
    
    print(f"\nConfigurations per Parameter:")
    for param, count in sorted(param_stats.items()):
        print(f"  • {param:<30} : {count:>3} configs")
    
    if disabled_params:
        print(f"\nDisabled Parameters ({len(disabled_params)}):")
        for param in sorted(disabled_params):
            print(f"  • {param}")
    
    if skipped_params:
        print(f"\nSkipped Configurations ({len(skipped_params)}):")
        for item in skipped_params:
            print(f"  • {item}")
    
    print(f"\nOutput Files:")
    print(f"  • Manifest:   {manifest_path.resolve()}")
    print(f"  • Job Map:    {job_map_path.resolve()}")
    print(f"  • JSON Files: {output_dir.resolve()}")
    print(f"  • File Count: {len(list(output_dir.glob('*.json')))} JSON files")
    
    print(f"\n{'='*80}")
    print(f"Phase I Complete - Ready for SLURM Submission")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    args = parse_args()
    generate_sweep(args)
