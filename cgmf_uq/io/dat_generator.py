#!/usr/bin/env python3
"""
CGMF .dat File Generator

Generates perturbed CGMF input files by:
1. Copying all files from source directory to output directory
2. Parsing the 4 target .dat files (gstrength_gdr, spinscaling, rta, tkemodel)
3. Applying scale factors to parameters
4. Writing modified files back to output directory

This script is designed to be called repeatedly in HPC workflows with different
scale factor configurations for sensitivity analysis and random sampling.

Usage:
    python dat_generator.py <output_dir> <target_zaid> [--scales-json <path>] [--source-dir <path>]
    
    Or programmatically:
    from cgmf_uq.io.dat_generator import generate_perturbed_dat_files
    generate_perturbed_dat_files(output_dir, target_zaid, scale_factors)
"""

from pathlib import Path
import json
import shutil
import argparse
from typing import Dict, Any, Optional
import sys

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from cgmf_uq.io.dat_parser import parse_dat_file, write_dat_file


# ============================================================================
# Default Configurations
# ============================================================================

DEFAULT_SOURCE_DIR = Path("data/cgmf_default")

# Default scale factors (no perturbation)
DEFAULT_SCALES = {
    'gstrength_gdr': {param: 1.0 for param in [
        'global_PSF_norm', 'E1_DArigo_E_const1', 'E1_DArigo_E_const2',
        'E1_DArigo_E_exp', 'E1_DArigo_W_factor', 'E1_DArigo_S_coef',
        'E1_DH0_E_const', 'E1_DH0_E_exp_mass', 'E1_DH0_E_exp_beta',
        'E1_DH0_W_const', 'E1_DH0_W_beta_coef', 'E1_DH0_S_coef',
        'E1_DH1_E_const', 'E1_DH1_E_exp_mass', 'E1_DH1_W_const',
        'E1_DH1_W_beta_coef', 'E1_DH1_S_coef', 'M1_E_const', 'M1_E_exp',
        'M1_W_val', 'M1_S_val', 'E2_E_const', 'E2_E_exp', 'E2_W_const',
        'E2_W_mass_coef', 'E2_S_coef'
    ]},
    'spinscaling': {
        'alpha_0_scale': 1.0,
        'alpha_slope_scale': 1.0,
    },
    'rta': {
        'scale_factor': 1.0,
    },
    'tkemodel': {
        'tke_en_scales': [1.0] * 4,
        'tke_ah_scales': [1.0] * 11,
        'sigma_tke_scales': [1.0] * 11,
    }
}

# Files that need to be modified (all others are copied verbatim)
TARGET_FILES = [
    'gstrength_gdr_params.dat',
    'spinscalingmodel.dat',
    'rta.dat',
    'tkemodel.dat'
]


# ============================================================================
# Core Generation Function
# ============================================================================

def generate_perturbed_dat_files(
    output_dir: Path,
    target_zaid: int,
    scale_factors: Optional[Dict[str, Any]] = None,
    source_dir: Path = DEFAULT_SOURCE_DIR,
    verbose: bool = True
) -> None:
    """
    Generate a complete set of CGMF .dat files with specified perturbations.
    
    This function:
    1. Creates output directory structure
    2. Copies ALL files from source to output
    3. Modifies the 4 target files with scale factors
    4. Preserves original formatting in modified files
    
    Args:
        output_dir: Destination directory for generated files
        target_zaid: ZAID of target nucleus (e.g., 92235 for U-235)
        scale_factors: Dict with scale factors for each file type (uses defaults if None)
        source_dir: Source directory containing baseline .dat files
        verbose: Print progress information
        
    Raises:
        FileNotFoundError: If source directory doesn't exist
        ValueError: If scale_factors have invalid structure
    """
    output_dir = Path(output_dir)
    source_dir = Path(source_dir)
    
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {source_dir}")
    
    # Use default scales if none provided
    if scale_factors is None:
        scale_factors = DEFAULT_SCALES
    
    # Validate scale_factors structure
    _validate_scale_factors(scale_factors)
    
    if verbose:
        print(f"Generating perturbed .dat files:")
        print(f"  Source:      {source_dir}")
        print(f"  Output:      {output_dir}")
        print(f"  Target ZAID: {target_zaid}")
    
    # Step 1: Create output directory and copy all files
    output_dir.mkdir(parents=True, exist_ok=True)
    _copy_all_files(source_dir, output_dir, verbose)
    
    # Step 2: Parse and modify each target file
    for filename in TARGET_FILES:
        input_file = source_dir / filename
        output_file = output_dir / filename
        
        if not input_file.exists():
            if verbose:
                print(f"  WARNING: Skipping {filename} (not found in source)")
            continue
        
        _process_target_file(
            filename, input_file, output_file,
            target_zaid, scale_factors, verbose
        )
    
    if verbose:
        print(f"✓ Successfully generated perturbed files in {output_dir}")


# ============================================================================
# Helper Functions
# ============================================================================

def _copy_all_files(source_dir: Path, output_dir: Path, verbose: bool) -> None:
    """Copy all files from source to output directory."""
    if verbose:
        print(f"  Copying all files from source...")
    
    copied_count = 0
    for item in source_dir.rglob('*'):
        if item.is_file():
            rel_path = item.relative_to(source_dir)
            dest_path = output_dir / rel_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dest_path)
            copied_count += 1
    
    if verbose:
        print(f"  Copied {copied_count} files")


def _process_target_file(
    filename: str,
    input_file: Path,
    output_file: Path,
    target_zaid: int,
    scale_factors: Dict[str, Any],
    verbose: bool
) -> None:
    """Parse, modify, and write a single target .dat file."""
    if verbose:
        print(f"  Processing {filename}...")
    
    file_type = _get_file_type(filename)
    
    # Parse with format preservation
    params, format_info = parse_dat_file(
        input_file,
        preserve_format=True,
        target_zaid=target_zaid
    )
    
    # Write with appropriate scale factors
    kwargs = {'target_zaid': target_zaid}
    
    if file_type == 'gstrength_gdr':
        kwargs['scale_factors'] = scale_factors['gstrength_gdr']
    
    elif file_type == 'spinscaling':
        kwargs['alpha_0_scale'] = scale_factors['spinscaling']['alpha_0_scale']
        kwargs['alpha_slope_scale'] = scale_factors['spinscaling']['alpha_slope_scale']
    
    elif file_type == 'rta':
        kwargs['scale_factor'] = scale_factors['rta']['scale_factor']
    
    elif file_type == 'tkemodel':
        kwargs['tke_en_scales'] = scale_factors['tkemodel']['tke_en_scales']
        kwargs['tke_ah_scales'] = scale_factors['tkemodel']['tke_ah_scales']
        kwargs['sigma_tke_scales'] = scale_factors['tkemodel']['sigma_tke_scales']
    
    write_dat_file(output_file, params, format_info, **kwargs)
    
    if verbose:
        print(f"    ✓ {filename}")


def _get_file_type(filename: str) -> str:
    """Determine file type from filename."""
    filename_lower = filename.lower()
    if 'gstrength_gdr' in filename_lower:
        return 'gstrength_gdr'
    elif 'spinscaling' in filename_lower:
        return 'spinscaling'
    elif 'rta' in filename_lower:
        return 'rta'
    elif 'tkemodel' in filename_lower:
        return 'tkemodel'
    else:
        raise ValueError(f"Unknown file type: {filename}")


def _validate_scale_factors(scale_factors: Dict[str, Any]) -> None:
    """Validate structure of scale_factors dictionary."""
    required_keys = {'gstrength_gdr', 'spinscaling', 'rta', 'tkemodel'}
    
    if not all(k in scale_factors for k in required_keys):
        missing = required_keys - set(scale_factors.keys())
        raise ValueError(f"scale_factors missing required keys: {missing}")
    
    # Validate gstrength_gdr has all 26 parameters
    if len(scale_factors['gstrength_gdr']) != 26:
        raise ValueError(
            f"gstrength_gdr must have 26 parameters, got {len(scale_factors['gstrength_gdr'])}"
        )
    
    # Validate spinscaling has 2 parameters
    spinscaling_keys = {'alpha_0_scale', 'alpha_slope_scale'}
    if set(scale_factors['spinscaling'].keys()) != spinscaling_keys:
        raise ValueError(f"spinscaling must have keys: {spinscaling_keys}")
    
    # Validate rta has scale_factor
    if 'scale_factor' not in scale_factors['rta']:
        raise ValueError("rta must have 'scale_factor' key")
    
    # Validate tkemodel array lengths
    tke = scale_factors['tkemodel']
    if len(tke.get('tke_en_scales', [])) != 4:
        raise ValueError("tkemodel.tke_en_scales must have 4 values")
    if len(tke.get('tke_ah_scales', [])) != 11:
        raise ValueError("tkemodel.tke_ah_scales must have 11 values")
    if len(tke.get('sigma_tke_scales', [])) != 11:
        raise ValueError("tkemodel.sigma_tke_scales must have 11 values")


# ============================================================================
# CLI Interface
# ============================================================================

def main():
    """Command-line interface for dat_generator."""
    parser = argparse.ArgumentParser(
        description='Generate perturbed CGMF .dat files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate with default scales (no perturbation)
  python dat_generator.py output/run_001 92235
  
  # Generate with custom scales from JSON file
  python dat_generator.py output/run_001 92235 --scales-json scales.json
  
  # Use custom source directory
  python dat_generator.py output/run_001 92235 --source-dir /path/to/cgmf_files

Scale factors JSON format:
  {
    "gstrength_gdr": {"global_PSF_norm": 1.1, "E1_DArigo_E_const1": 0.9, ...},
    "spinscaling": {"alpha_0_scale": 1.05, "alpha_slope_scale": 1.0},
    "rta": {"scale_factor": 0.95},
    "tkemodel": {
      "tke_en_scales": [1.0, 1.0, 1.0, 1.0],
      "tke_ah_scales": [1.0, 1.0, ...],
      "sigma_tke_scales": [1.0, 1.0, ...]
    }
  }
        """
    )
    
    parser.add_argument(
        'output_dir',
        type=Path,
        help='Output directory for generated files'
    )
    
    parser.add_argument(
        'target_zaid',
        type=int,
        help='Target ZAID (e.g., 92235 for U-235)'
    )
    
    parser.add_argument(
        '--scales-json',
        type=Path,
        help='JSON file containing scale factors (uses defaults if not provided)'
    )
    
    parser.add_argument(
        '--source-dir',
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f'Source directory with baseline .dat files (default: {DEFAULT_SOURCE_DIR})'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress progress output'
    )
    
    args = parser.parse_args()
    
    # Load scale factors if provided
    scale_factors = None
    if args.scales_json:
        if not args.scales_json.exists():
            print(f"ERROR: Scale factors file not found: {args.scales_json}")
            sys.exit(1)
        
        with open(args.scales_json, 'r') as f:
            scale_factors = json.load(f)
    
    # Generate files
    try:
        generate_perturbed_dat_files(
            output_dir=args.output_dir,
            target_zaid=args.target_zaid,
            scale_factors=scale_factors,
            source_dir=args.source_dir,
            verbose=not args.quiet
        )
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

