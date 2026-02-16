#!/usr/bin/env python3
"""
Verify uniqueness of CGMF post-processing results across sensitivity sweep tasks.

Compares the RESULTS section of postproc.log files to detect duplicate runs.
Designed to run on both local machines and HPC environments with minimal dependencies.

Usage:
    python verify_unique_results.py [--runs-dir RUNS_DIR] [--output OUTPUT] [--verbose]
"""

import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, Tuple, Optional, List


def extract_task_number(task_name: str) -> int:
    """Extract the numeric part of a task name for proper sorting."""
    match = re.search(r'task_(\d+)', task_name)
    if match:
        return int(match.group(1))
    return 0


def sort_task_names(task_names: List[str]) -> List[str]:
    """Sort task names numerically rather than alphabetically."""
    return sorted(task_names, key=extract_task_number)


def extract_results_section(log_content: str) -> Optional[str]:
    """
    Extract the RESULTS section from a postproc.log file.
    
    Returns the normalized content between the RESULTS delimiters,
    or None if the section cannot be found.
    """
    pattern = r"RESULTS:.*?─{70}(.*?)─{70}"
    match = re.search(pattern, log_content, re.DOTALL)
    
    if not match:
        return None
    
    results = match.group(1).strip()
    
    # Normalize whitespace for comparison
    results = re.sub(r'\s+', ' ', results)
    
    return results


def parse_results_fields(results: str) -> Dict[str, str]:
    """
    Parse the normalized results string into individual fields for display.
    """
    fields = {}
    
    # Extract key-value pairs
    patterns = {
	'total_gammas': r'Total Gammas:\s*([\d,]+)',      # ← Allow commas
	'total_neutrons': r'Total Neutrons:\s*([\d,]+)',  # ← Allow commas
        'gamma_mult': r'ν̄ \(Gamma Multiplicity\):\s*([\d.]+)\s*γ/fission',
        'neutron_mult': r'ν̄ \(Neutron Multiplicity\):\s*([\d.]+)\s*n/fission',
        'single_gamma_e': r'ε̄ \(Single Gamma Energy\):\s*([\d.]+)\s*MeV',
        'total_gamma_e': r'ε̄ \(Total Gamma Energy\):\s*([\d.]+)\s*MeV/fission',
        'gamma_mult_range': r'Gamma multiplicity range:\s*([\d\s-]+)',
        'neutron_mult_range': r'Neutron multiplicity range:\s*([\d\s-]+)',
        'most_prob_gamma_total': r'Most probable γ \(total\):\s*(\d+)',
        'most_prob_gamma_light': r'Most probable γ \(light fragment\):\s*(\d+)',
        'most_prob_gamma_heavy': r'Most probable γ \(heavy fragment\):\s*(\d+)',
        'most_prob_n_total': r'Most probable n \(total\):\s*(\d+)',
        'most_prob_n_light': r'Most probable n \(light fragment\):\s*(\d+)',
        'most_prob_n_heavy': r'Most probable n \(heavy fragment\):\s*(\d+)',
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, results)
        if match:
            fields[key] = match.group(1).strip()
    
    return fields


def format_results_fingerprint(results: str) -> str:
    """Format the results fingerprint with clear structure."""
    fields = parse_results_fields(results)
    
    lines = []
    lines.append("  ┌─────────────────────────────────────────────────────────────────────")
    lines.append("  │ PARTICLE COUNTS:")
    lines.append("  ├─────────────────────────────────────────────────────────────────────")
    lines.append(f"  │   Total Gammas:              {fields.get('total_gammas', 'N/A')}")
    lines.append(f"  │   Total Neutrons:            {fields.get('total_neutrons', 'N/A')}")
    lines.append("  │")
    lines.append("  │ MULTIPLICITIES:")
    lines.append("  ├─────────────────────────────────────────────────────────────────────")
    lines.append(f"  │   ν̄ (Gamma):                 {fields.get('gamma_mult', 'N/A')} γ/fission")
    lines.append(f"  │   ν̄ (Neutron):               {fields.get('neutron_mult', 'N/A')} n/fission")
    lines.append("  │")
    lines.append("  │ ENERGIES:")
    lines.append("  ├─────────────────────────────────────────────────────────────────────")
    lines.append(f"  │   ε̄ (Single Gamma):          {fields.get('single_gamma_e', 'N/A')} MeV")
    lines.append(f"  │   ε̄ (Total Gamma):           {fields.get('total_gamma_e', 'N/A')} MeV/fission")
    lines.append("  │")
    lines.append("  │ MULTIPLICITY DISTRIBUTIONS:")
    lines.append("  ├─────────────────────────────────────────────────────────────────────")
    lines.append(f"  │   Gamma range:               {fields.get('gamma_mult_range', 'N/A')}")
    lines.append(f"  │   Neutron range:             {fields.get('neutron_mult_range', 'N/A')}")
    lines.append("  │")
    lines.append("  │ MOST PROBABLE VALUES:")
    lines.append("  ├─────────────────────────────────────────────────────────────────────")
    lines.append(f"  │   γ (total):                 {fields.get('most_prob_gamma_total', 'N/A')}")
    lines.append(f"  │   γ (light fragment):        {fields.get('most_prob_gamma_light', 'N/A')}")
    lines.append(f"  │   γ (heavy fragment):        {fields.get('most_prob_gamma_heavy', 'N/A')}")
    lines.append(f"  │   n (total):                 {fields.get('most_prob_n_total', 'N/A')}")
    lines.append(f"  │   n (light fragment):        {fields.get('most_prob_n_light', 'N/A')}")
    lines.append(f"  │   n (heavy fragment):        {fields.get('most_prob_n_heavy', 'N/A')}")
    lines.append("  └─────────────────────────────────────────────────────────────────────")
    
    return "\n".join(lines)


def verify_uniqueness(runs_dir: Path, verbose: bool = False) -> Tuple[bool, Dict]:
    """
    Verify that all postproc.log RESULTS sections are unique.
    
    Returns:
        (all_unique, analysis_dict) where analysis_dict contains:
            - 'duplicates': list of duplicate groups
            - 'failed_to_parse': list of tasks that couldn't be parsed
            - 'missing_logs': list of tasks with no postproc.log
            - 'total_tasks': total number of task directories found
    """
    if not runs_dir.exists():
        print(f"ERROR: Directory not found: {runs_dir}")
        sys.exit(1)
    
    # Get all task directories and sort them numerically
    task_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and d.name.startswith('task_')]
    task_dirs = sorted(task_dirs, key=lambda d: extract_task_number(d.name))
    
    if not task_dirs:
        print(f"ERROR: No task directories found in {runs_dir}")
        sys.exit(1)
    
    print(f"Found {len(task_dirs)} task directories")
    print("=" * 80)
    
    results_map = defaultdict(list)  # results_hash -> [task_names]
    failed_to_parse = []
    missing_logs = []
    
    # Process each task
    for task_dir in task_dirs:
        task_name = task_dir.name
        log_file = task_dir / "postproc.log"
        
        if not log_file.exists():
            missing_logs.append(task_name)
            if verbose:
                print(f"⚠ {task_name}: postproc.log not found")
            continue
        
        try:
            log_content = log_file.read_text()
            results = extract_results_section(log_content)
            
            if results is None:
                failed_to_parse.append(task_name)
                if verbose:
                    print(f"⚠ {task_name}: Could not extract RESULTS section")
                continue
            
            # Use the full results string as the key for exact matching
            results_map[results].append(task_name)
            
            if verbose:
                print(f"✓ {task_name}: Processed successfully")
                
        except Exception as e:
            failed_to_parse.append(task_name)
            print(f"✗ {task_name}: Error reading file - {e}")
    
    # Find duplicates and sort task lists numerically
    duplicates = []
    for results, tasks in results_map.items():
        if len(tasks) > 1:
            duplicates.append((results, sort_task_names(tasks)))
    
    # Sort duplicate groups by the first task number in each group
    duplicates.sort(key=lambda x: extract_task_number(x[1][0]))
    
    # Prepare analysis
    analysis = {
        'duplicates': duplicates,
        'failed_to_parse': sort_task_names(failed_to_parse),
        'missing_logs': sort_task_names(missing_logs),
        'total_tasks': len(task_dirs),
        'successfully_parsed': len(task_dirs) - len(failed_to_parse) - len(missing_logs)
    }
    
    all_unique = len(duplicates) == 0
    
    return all_unique, analysis


def format_report(all_unique: bool, analysis: Dict) -> str:
    """Format the verification results as a string report."""
    
    lines = []
    lines.append("═" * 80)
    lines.append("█                    VERIFICATION SUMMARY                               █")
    lines.append("═" * 80)
    lines.append(f"  Total tasks:          {analysis['total_tasks']}")
    lines.append(f"  Successfully parsed:  {analysis['successfully_parsed']}")
    lines.append(f"  Failed to parse:      {len(analysis['failed_to_parse'])}")
    lines.append(f"  Missing logs:         {len(analysis['missing_logs'])}")
    lines.append("═" * 80)
    lines.append("")
    
    if all_unique:
        lines.append("✓ ALL RESULTS ARE UNIQUE")
        lines.append("  No duplicate RESULTS sections found.")
        lines.append("")
    else:
        lines.append("✗ DUPLICATES DETECTED")
        lines.append(f"  Found {len(analysis['duplicates'])} groups of duplicate results")
        lines.append("")
        lines.append("═" * 80)
        
        for i, (results, tasks) in enumerate(analysis['duplicates'], 1):
            lines.append("")
            lines.append("─" * 80)
            lines.append(f"█ DUPLICATE GROUP {i} │ {len(tasks)} identical runs")
            lines.append("─" * 80)
            lines.append(f"  Tasks: {', '.join(tasks)}")
            lines.append("")
            lines.append(format_results_fingerprint(results))
            lines.append("")
    
    # Report issues
    if analysis['missing_logs']:
        lines.append("─" * 80)
        lines.append("█ MISSING LOGS")
        lines.append("─" * 80)
        for task in analysis['missing_logs']:
            lines.append(f"  • {task}")
        lines.append("")
    
    if analysis['failed_to_parse']:
        lines.append("─" * 80)
        lines.append("█ FAILED TO PARSE")
        lines.append("─" * 80)
        for task in analysis['failed_to_parse']:
            lines.append(f"  • {task}")
        lines.append("")
    
    lines.append("═" * 80)
    
    return "\n".join(lines)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Verify uniqueness of CGMF post-processing results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python verify_unique_results.py
  python verify_unique_results.py --runs-dir /path/to/runs --output results_check.txt
  python verify_unique_results.py --verbose
        """
    )
    parser.add_argument(
        '--runs-dir',
        type=Path,
        default=Path('runs'),
        help='Path to runs directory (default: ./runs)'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=None,
        help='Output file path (default: print to stdout only)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print detailed progress for each task'
    )
    
    args = parser.parse_args()
    
    print("═" * 80)
    print("█           CGMF Results Uniqueness Verification                        █")
    print("═" * 80)
    print(f"  Runs directory: {args.runs_dir.resolve()}")
    if args.output:
        print(f"  Output file:    {args.output.resolve()}")
    print("")
    
    all_unique, analysis = verify_uniqueness(args.runs_dir, args.verbose)
    report = format_report(all_unique, analysis)
    
    # Print to stdout
    print(report)
    
    # Write to file if specified
    if args.output:
        try:
            args.output.write_text(report)
            print(f"\n✓ Report written to: {args.output}")
        except Exception as e:
            print(f"\n✗ Failed to write output file: {e}")
            sys.exit(1)
    
    # Exit with appropriate code
    if not all_unique or analysis['failed_to_parse']:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

