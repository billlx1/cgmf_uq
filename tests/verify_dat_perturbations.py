#!/usr/bin/env python3
"""
CGMFDat File Perturbation Verification Script
===============================================
Verifies that dat files in each task run were correctly perturbed from baseline.

Usage:
    python verify_dat_perturbations.py --baseline /path/to/CGMF_Data_Default --runs ./runs

This script compares each task's dat files against the baseline to identify:
- Which parameters were modified
- Magnitude of changes
- Files that are unexpectedly identical or missing
- Potential silent failures (all files identical = likely failure)

Output: Structured summary for each task + overall statistics
"""

import os
import sys
import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple, Set
from collections import defaultdict
import re


def natural_sort_key(text: str) -> List:
    """
    Generate a key for natural sorting (handles numbers properly).
    Converts 'task_9' < 'task_10' instead of 'task_10' < 'task_9'
    """
    def atoi(text):
        return int(text) if text.isdigit() else text
    
    return [atoi(c) for c in re.split(r'(\d+)', str(text))]


class DatFileComparator:
    """Handles comparison of individual .dat files"""
    
    # Files that should exist for valid CGMF runs
    REQUIRED_FILES = {
        'deformations.dat',
        'gstrength_gdr_params.dat',
        'kcksyst.dat',
        'rta.dat',
        'spinscalingmodel.dat',
        'tkemodel.dat',
        'yamodel.dat'
    }
    
    # Files that require ZAID-specific comparison (due to structural rearrangement during write)
    ZAID_COMPARISON_FILES = {
        'tkemodel.dat',
        'spinscalingmodel.dat'
    }
    
    def __init__(self, baseline_dir: Path, target_zaid: str = None):
        self.baseline_dir = baseline_dir
        self.baseline_cache = {}
        self.target_zaid = target_zaid  # e.g., "92236" for U-235(n,f)
        self._load_baseline_files()
    
    def _load_baseline_files(self):
        """Pre-load all baseline files for efficiency"""
        for filename in self.REQUIRED_FILES:
            filepath = self.baseline_dir / filename
            if filepath.exists():
                with open(filepath, 'r') as f:
                    self.baseline_cache[filename] = f.readlines()
            else:
                print(f"WARNING: Baseline file missing: {filename}")
    
    def _extract_zaid_line(self, lines: List[str], zaid: str) -> Tuple[str, int]:
        """
        Extract the data line for a specific ZAID from tkemodel or spinscaling files.
        Returns (line_content, line_number) or (None, -1) if not found.
        """
        for i, line in enumerate(lines):
            # Skip comments and empty lines
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue
            
            # Check if line starts with the ZAID (including negative for spontaneous fission)
            # Split on whitespace and check first token
            tokens = stripped.split()
            if tokens and tokens[0] == zaid:
                return (line, i + 1)
        
        return (None, -1)
    
    def compare_files(self, task_file: Path, baseline_filename: str) -> Dict:
        """
        Compare a task's dat file against baseline.
        Returns dict with difference metrics.
        
        For tkemodel.dat and spinscalingmodel.dat, only compares the target ZAID line.
        """
        if baseline_filename not in self.baseline_cache:
            return {'status': 'baseline_missing', 'differences': []}
        
        if not task_file.exists():
            return {'status': 'task_missing', 'differences': []}
        
        with open(task_file, 'r') as f:
            task_lines = f.readlines()
        
        baseline_lines = self.baseline_cache[baseline_filename]
        
        # Special handling for ZAID-specific comparison files
        if baseline_filename in self.ZAID_COMPARISON_FILES:
            if not self.target_zaid:
                return {
                    'status': 'no_zaid_specified',
                    'differences': [],
                    'message': f'{baseline_filename} requires target_zaid for comparison'
                }
            
            return self._compare_zaid_lines(task_lines, baseline_lines, baseline_filename)
        
        # Standard full-file comparison for other files
        # Quick check for identical files
        if task_lines == baseline_lines:
            return {'status': 'identical', 'differences': []}
        
        # Find actual differences
        differences = []
        max_lines = max(len(task_lines), len(baseline_lines))
        
        for i in range(max_lines):
            baseline_line = baseline_lines[i] if i < len(baseline_lines) else ""
            task_line = task_lines[i] if i < len(task_lines) else ""
            
            if baseline_line != task_line:
                # Extract numeric values for quantitative comparison
                baseline_nums = self._extract_numbers(baseline_line)
                task_nums = self._extract_numbers(task_line)
                
                # Skip lines that are numerically equivalent (only formatting differs)
                if self._are_numerically_equivalent(baseline_nums, task_nums):
                    continue
                
                differences.append({
                    'line_num': i + 1,
                    'baseline': baseline_line.rstrip(),
                    'task': task_line.rstrip(),
                    'baseline_values': baseline_nums,
                    'task_values': task_nums
                })
        
        # If no meaningful differences found, treat as identical
        if not differences:
            return {'status': 'identical', 'differences': []}
        
        return {'status': 'modified', 'differences': differences}
    
    def _compare_zaid_lines(self, task_lines: List[str], baseline_lines: List[str], 
                           filename: str) -> Dict:
        """
        Compare only the specific ZAID line between baseline and task.
        """
        baseline_line, baseline_line_num = self._extract_zaid_line(baseline_lines, self.target_zaid)
        task_line, task_line_num = self._extract_zaid_line(task_lines, self.target_zaid)
        
        # Check if ZAID was found in both files
        if baseline_line is None:
            return {
                'status': 'zaid_not_in_baseline',
                'differences': [],
                'message': f'ZAID {self.target_zaid} not found in baseline {filename}'
            }
        
        if task_line is None:
            return {
                'status': 'zaid_not_in_task',
                'differences': [],
                'message': f'ZAID {self.target_zaid} not found in task {filename}'
            }
        
        # Compare the lines
        if baseline_line == task_line:
            return {'status': 'identical', 'differences': []}
        
        # Extract numeric values for comparison
        baseline_nums = self._extract_numbers(baseline_line)
        task_nums = self._extract_numbers(task_line)
        
        # Check if values are numerically equivalent (only formatting differs)
        if self._are_numerically_equivalent(baseline_nums, task_nums):
            return {'status': 'identical', 'differences': []}
        
        differences = [{
            'line_num': f'ZAID {self.target_zaid} (baseline L{baseline_line_num}, task L{task_line_num})',
            'baseline': baseline_line.rstrip(),
            'task': task_line.rstrip(),
            'baseline_values': baseline_nums,
            'task_values': task_nums
        }]
        
        return {'status': 'modified', 'differences': differences}
    
    @staticmethod
    def _extract_numbers(line: str) -> List[float]:
        """Extract all numeric values from a line"""
        # Match scientific notation and regular floats
        pattern = r'[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?'
        matches = re.findall(pattern, line)
        try:
            return [float(m) for m in matches]
        except ValueError:
            return []
    
    @staticmethod
    def _are_numerically_equivalent(baseline_nums: List[float], task_nums: List[float], 
                                   threshold: float = 0.01) -> bool:
        """
        Check if two lists of numbers are numerically equivalent.
        Returns True if all percentage changes are below threshold (default 0.01%).
        This handles cases where only formatting differs (e.g., 171.74 vs 1.717400E+02).
        """
        if len(baseline_nums) != len(task_nums):
            return False
        
        if not baseline_nums:  # Empty lists
            return True
        
        for b_val, t_val in zip(baseline_nums, task_nums):
            # Handle exact equality (including zeros)
            if b_val == t_val:
                continue
            
            # For non-zero baseline values, check percentage change
            if b_val != 0:
                pct_change = abs((t_val - b_val) / b_val) * 100
                if pct_change >= threshold:
                    return False
            else:
                # If baseline is zero but task is not, they're different
                if abs(t_val) > 1e-15:  # Small tolerance for floating point
                    return False
        
        return True


class TaskAnalyzer:
    """Analyzes a single task directory"""
    
    def __init__(self, task_dir: Path, comparator: DatFileComparator, target_zaid: str = None):
        self.task_dir = task_dir
        self.task_id = task_dir.name
        self.comparator = comparator
        self.dat_files_dir = task_dir / "dat_files"
        self.target_zaid = target_zaid
    
    def analyze(self) -> Dict:
        """Perform complete analysis of this task"""
        result = {
            'task_id': self.task_id,
            'exists': self.task_dir.exists(),
            'dat_dir_exists': False,
            'files': {},
            'summary': {}
        }
        
        if not self.task_dir.exists():
            result['summary']['status'] = 'MISSING'
            return result
        
        if not self.dat_files_dir.exists():
            result['summary']['status'] = 'NO_DAT_DIR'
            return result
        
        result['dat_dir_exists'] = True
        
        # Check each required file
        modified_files = []
        identical_files = []
        missing_files = []
        zaid_issues = []
        
        for filename in DatFileComparator.REQUIRED_FILES:
            task_file = self.dat_files_dir / filename
            comparison = self.comparator.compare_files(task_file, filename)
            result['files'][filename] = comparison
            
            # Handle different statuses
            status = comparison['status']
            
            if status == 'modified':
                modified_files.append(filename)
            elif status == 'identical':
                identical_files.append(filename)
            elif status == 'task_missing':
                missing_files.append(filename)
            elif status in ['zaid_not_in_baseline', 'zaid_not_in_task', 'no_zaid_specified']:
                zaid_issues.append(f"{filename}: {comparison.get('message', status)}")
        
        # Determine overall status
        result['summary']['modified_count'] = len(modified_files)
        result['summary']['identical_count'] = len(identical_files)
        result['summary']['missing_count'] = len(missing_files)
        result['summary']['modified_files'] = modified_files
        result['summary']['identical_files'] = identical_files
        result['summary']['missing_files'] = missing_files
        
        if zaid_issues:
            result['summary']['zaid_issues'] = zaid_issues
        
        # Flag potential issues
        if len(missing_files) > 0:
            result['summary']['status'] = 'INCOMPLETE'
        elif zaid_issues:
            result['summary']['status'] = 'ZAID_ISSUE'
        elif len(modified_files) == 0:
            result['summary']['status'] = 'SILENT_FAILURE'  # All identical = likely failed
        else:
            result['summary']['status'] = 'SUCCESS'
        
        return result


class VerificationReport:
    """Generates human-readable verification reports"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_task_report(self, analysis: Dict, verbose: bool = False):
        """Generate detailed report for a single task"""
        task_id = analysis['task_id']
        report_file = self.output_dir / f"{task_id}_verification.txt"
        
        with open(report_file, 'w') as f:
            f.write(f"{'='*80}\n")
            f.write(f"VERIFICATION REPORT: {task_id}\n")
            f.write(f"{'='*80}\n\n")
            
            summary = analysis.get('summary', {})
            status = summary.get('status', 'UNKNOWN')
            
            f.write(f"Status: {status}\n")
            f.write(f"Modified Files: {summary.get('modified_count', 0)}\n")
            f.write(f"Identical Files: {summary.get('identical_count', 0)}\n")
            f.write(f"Missing Files: {summary.get('missing_count', 0)}\n\n")
            
            if status == 'SILENT_FAILURE':
                f.write("⚠️  WARNING: No files modified - possible silent failure!\n\n")
            
            if status == 'ZAID_ISSUE':
                f.write("⚠️  WARNING: ZAID comparison issues detected:\n")
                for issue in summary.get('zaid_issues', []):
                    f.write(f"    {issue}\n")
                f.write("\n")
            
            # List modified files
            if summary.get('modified_files'):
                f.write("Modified Files:\n")
                for filename in summary['modified_files']:
                    f.write(f"  ✓ {filename}\n")
                f.write("\n")
            
            # List issues
            if summary.get('missing_files'):
                f.write("Missing Files:\n")
                for filename in summary['missing_files']:
                    f.write(f"  ✗ {filename}\n")
                f.write("\n")
            
            # Detailed differences (if verbose)
            if verbose:
                f.write(f"\n{'='*80}\n")
                f.write("DETAILED DIFFERENCES\n")
                f.write(f"{'='*80}\n\n")
                
                for filename, comparison in analysis.get('files', {}).items():
                    if comparison['status'] == 'modified':
                        f.write(f"\n{filename}:\n")
                        f.write(f"{'-'*80}\n")
                        
                        diffs = comparison.get('differences', [])
                        
                        # Special note for ZAID-compared files
                        if filename in DatFileComparator.ZAID_COMPARISON_FILES:
                            f.write(f"  NOTE: ZAID-specific comparison (only target ZAID line compared)\n\n")
                        
                        # Note for columnar files
                        if filename in ['kcksyst.dat', 'deformations.dat']:
                            f.write(f"  NOTE: Full-file comparison (scale factors applied to specific columns)\n\n")
                        
                        # Show total count if many differences
                        if len(diffs) > 20:
                            f.write(f"  Total differences: {len(diffs)} lines\n\n")
                        
                        # Show ALL differences, no limit
                        for diff in diffs:
                            line_info = diff['line_num']
                            f.write(f"  Line {line_info}:\n")
                            f.write(f"    Baseline: {diff['baseline']}\n")
                            f.write(f"    Task:     {diff['task']}\n")
                            
                            # Show numeric change if applicable
                            if diff['baseline_values'] and diff['task_values']:
                                if len(diff['baseline_values']) == len(diff['task_values']):
                                    for b_val, t_val in zip(diff['baseline_values'], diff['task_values']):
                                        if b_val != 0:
                                            pct_change = ((t_val - b_val) / b_val) * 100
                                            f.write(f"    Change: {b_val:.6e} → {t_val:.6e} ({pct_change:+.2f}%)\n")
                            f.write("\n")
        
        return report_file
    
    def generate_consolidated_verbose_report(self, all_analyses: List[Dict]):
        """Generate a single consolidated report with all task details for easy reading"""
        report_file = self.output_dir / "CONSOLIDATED_VERIFICATION_REPORT.txt"
        
        # Sort analyses by task_id naturally
        sorted_analyses = sorted(all_analyses, key=lambda x: natural_sort_key(x['task_id']))
        
        with open(report_file, 'w') as f:
            f.write(f"{'#'*80}\n")
            f.write(f"{'#'*80}\n")
            f.write(f"###  CONSOLIDATED VERIFICATION REPORT - ALL TASKS\n")
            f.write(f"{'#'*80}\n")
            f.write(f"{'#'*80}\n\n")
            
            for i, analysis in enumerate(sorted_analyses, 1):
                task_id = analysis['task_id']
                summary = analysis.get('summary', {})
                status = summary.get('status', 'UNKNOWN')
                
                # Visual separator between tasks
                f.write(f"\n\n{'█'*80}\n")
                f.write(f"█ [{i}/{len(sorted_analyses)}] {task_id:20s} │ Status: {status:20s} █\n")
                f.write(f"{'█'*80}\n\n")
                
                # Status summary
                f.write(f"Modified: {summary.get('modified_count', 0)} │ ")
                f.write(f"Identical: {summary.get('identical_count', 0)} │ ")
                f.write(f"Missing: {summary.get('missing_count', 0)}\n\n")
                
                # Warnings
                if status == 'SILENT_FAILURE':
                    f.write("⚠️  SILENT FAILURE - No files modified!\n\n")
                
                if status == 'ZAID_ISSUE':
                    f.write("⚠️  ZAID ISSUES:\n")
                    for issue in summary.get('zaid_issues', []):
                        f.write(f"    • {issue}\n")
                    f.write("\n")
                
                # List modified files
                if summary.get('modified_files'):
                    f.write("Modified Files:\n")
                    for filename in summary['modified_files']:
                        f.write(f"  ✓ {filename}\n")
                    f.write("\n")
                
                # Show detailed differences
                has_diffs = False
                for filename, comparison in analysis.get('files', {}).items():
                    if comparison['status'] == 'modified':
                        if not has_diffs:
                            f.write(f"{'─'*80}\n")
                            f.write(f"DETAILED CHANGES:\n")
                            f.write(f"{'─'*80}\n\n")
                            has_diffs = True
                        
                        f.write(f"┌─ {filename}\n")
                        
                        # Special notes
                        if filename in DatFileComparator.ZAID_COMPARISON_FILES:
                            f.write(f"│  [ZAID-specific comparison]\n")
                        elif filename in ['kcksyst.dat', 'deformations.dat']:
                            f.write(f"│  [Columnar file - scale factors applied to specific columns]\n")
                        
                        diffs = comparison.get('differences', [])
                        
                        # Show more lines in consolidated report (for overview across all tasks)
                        # Full details available in individual task reports
                        show_count = min(len(diffs), 50)  # Show up to 50 lines per file
                        if len(diffs) > show_count:
                            f.write(f"│  Total: {len(diffs)} changes (showing first {show_count})\n")
                            f.write(f"│  See task_{task_id.split('_')[1]}_verification.txt for complete list\n")
                        
                        f.write("│\n")
                        
                        for diff in diffs[:show_count]:
                            line_info = diff['line_num']
                            f.write(f"│  Line {line_info}:\n")
                            
                            # Show numeric changes compactly
                            if diff['baseline_values'] and diff['task_values']:
                                if len(diff['baseline_values']) == len(diff['task_values']):
                                    changes = []
                                    for b_val, t_val in zip(diff['baseline_values'], diff['task_values']):
                                        if b_val != 0:
                                            pct_change = ((t_val - b_val) / b_val) * 100
                                            if abs(pct_change) >= 0.01:  # Only show meaningful changes
                                                changes.append(f"{b_val:.3e}→{t_val:.3e} ({pct_change:+.1f}%)")
                                    
                                    if changes:
                                        f.write(f"│    {', '.join(changes[:3])}\n")  # Show first 3 changes
                                        if len(changes) > 3:
                                            f.write(f"│    ... and {len(changes)-3} more\n")
                            
                            f.write("│\n")
                        
                        f.write(f"└{'─'*79}\n\n")
        
        return report_file
    
    def generate_summary_report(self, all_analyses: List[Dict]):
        """Generate overall summary across all tasks"""
        summary_file = self.output_dir / "VERIFICATION_SUMMARY.txt"
        
        # Sort analyses by task_id naturally (task_1, task_2, ..., task_10, not task_1, task_10, task_2)
        sorted_analyses = sorted(all_analyses, key=lambda x: natural_sort_key(x['task_id']))
        
        # Collect statistics
        status_counts = defaultdict(int)
        tasks_by_modified_count = defaultdict(list)
        
        for analysis in sorted_analyses:
            status = analysis.get('summary', {}).get('status', 'UNKNOWN')
            status_counts[status] += 1
            
            modified_count = analysis.get('summary', {}).get('modified_count', 0)
            tasks_by_modified_count[modified_count].append(analysis['task_id'])
        
        with open(summary_file, 'w') as f:
            f.write(f"{'='*80}\n")
            f.write(f"OVERALL VERIFICATION SUMMARY\n")
            f.write(f"{'='*80}\n\n")
            
            f.write(f"Total Tasks Analyzed: {len(sorted_analyses)}\n\n")
            
            f.write("Status Breakdown:\n")
            for status, count in sorted(status_counts.items()):
                f.write(f"  {status:20s}: {count:4d}\n")
            f.write("\n")
            
            f.write("Distribution by Number of Modified Files:\n")
            for mod_count in sorted(tasks_by_modified_count.keys()):
                tasks = tasks_by_modified_count[mod_count]
                f.write(f"  {mod_count} files modified: {len(tasks):4d} tasks\n")
            f.write("\n")
            
            # Flag problematic tasks (sorted naturally)
            silent_failures = [a for a in sorted_analyses 
                             if a.get('summary', {}).get('status') == 'SILENT_FAILURE']
            if silent_failures:
                f.write(f"\n⚠️  SILENT FAILURES DETECTED ({len(silent_failures)} tasks):\n")
                for analysis in silent_failures:  # List ALL tasks, no limit
                    f.write(f"  - {analysis['task_id']}\n")
                f.write("\n")
            
            incomplete = [a for a in sorted_analyses 
                         if a.get('summary', {}).get('status') == 'INCOMPLETE']
            if incomplete:
                f.write(f"\n⚠️  INCOMPLETE TASKS ({len(incomplete)} tasks):\n")
                for analysis in incomplete:  # List ALL tasks, no limit
                    missing = analysis.get('summary', {}).get('missing_files', [])
                    f.write(f"  - {analysis['task_id']}: missing {missing}\n")
                f.write("\n")
            
            # Add ZAID issues section if any exist
            zaid_issues = [a for a in sorted_analyses 
                          if a.get('summary', {}).get('status') == 'ZAID_ISSUE']
            if zaid_issues:
                f.write(f"\n⚠️  ZAID ISSUES ({len(zaid_issues)} tasks):\n")
                for analysis in zaid_issues:  # List ALL tasks, no limit
                    issues = analysis.get('summary', {}).get('zaid_issues', [])
                    f.write(f"  - {analysis['task_id']}: {'; '.join(issues)}\n")
                f.write("\n")
        
        return summary_file


def main():
    parser = argparse.ArgumentParser(
        description='Verify CGMF dat file perturbations against baseline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard comparison (all files)
  python verify_dat_perturbations.py --baseline /path/to/CGMF_Data_Default --runs ./runs
  
  # With ZAID-specific comparison for tkemodel.dat and spinscalingmodel.dat
  python verify_dat_perturbations.py --baseline /path/to/CGMF_Data_Default --runs ./runs --zaid 92236
  
  # Verbose output with detailed diffs
  python verify_dat_perturbations.py --baseline /path/to/CGMF_Data_Default --runs ./runs --zaid 92236 --verbose
        """
    )
    parser.add_argument(
        '--baseline',
        type=Path,
        required=True,
        help='Path to baseline CGMF_Data_Default directory'
    )
    parser.add_argument(
        '--runs',
        type=Path,
        default=Path('./runs'),
        help='Path to runs directory (default: ./runs)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('./verification_reports'),
        help='Output directory for reports (default: ./verification_reports)'
    )
    parser.add_argument(
        '--zaid',
        type=str,
        default=None,
        help='Target ZAID for tkemodel.dat and spinscalingmodel.dat comparison (e.g., 92236 for U-235). Required for proper verification of these files.'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Generate detailed diff reports for each task'
    )
    parser.add_argument(
        '--tasks',
        nargs='+',
        help='Specific task IDs to verify (default: all tasks in runs/)'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.baseline.exists():
        print(f"ERROR: Baseline directory not found: {args.baseline}")
        sys.exit(1)
    
    if not args.runs.exists():
        print(f"ERROR: Runs directory not found: {args.runs}")
        sys.exit(1)
    
    print(f"{'='*80}")
    print(f"CGMF Dat File Verification")
    print(f"{'='*80}")
    print(f"Baseline: {args.baseline}")
    print(f"Runs:     {args.runs}")
    print(f"Output:   {args.output}")
    if args.zaid:
        print(f"ZAID:     {args.zaid} (for tkemodel.dat and spinscalingmodel.dat)")
    else:
        print(f"ZAID:     Not specified (tkemodel.dat and spinscalingmodel.dat will use full-file comparison)")
    print(f"{'='*80}\n")
    
    # Initialize comparator
    comparator = DatFileComparator(args.baseline, target_zaid=args.zaid)
    
    # Get task directories
    if args.tasks:
        task_dirs = [args.runs / task_id for task_id in args.tasks]
        # Sort naturally in case user provided them out of order
        task_dirs = sorted(task_dirs, key=lambda p: natural_sort_key(p.name))
    else:
        task_dirs = [d for d in args.runs.iterdir() if d.is_dir() and d.name.startswith('task_')]
        # Sort naturally (task_1, task_2, ..., task_10, not task_1, task_10, task_2)
        task_dirs = sorted(task_dirs, key=lambda p: natural_sort_key(p.name))
    
    print(f"Found {len(task_dirs)} tasks to verify\n")
    
    # Analyze each task
    all_analyses = []
    reporter = VerificationReport(args.output)
    
    for i, task_dir in enumerate(task_dirs, 1):
        print(f"[{i}/{len(task_dirs)}] Analyzing {task_dir.name}...", end='')
        
        analyzer = TaskAnalyzer(task_dir, comparator, target_zaid=args.zaid)
        analysis = analyzer.analyze()
        all_analyses.append(analysis)
        
        # Generate individual report
        if args.verbose:
            reporter.generate_task_report(analysis, verbose=True)
        
        status = analysis.get('summary', {}).get('status', 'UNKNOWN')
        print(f" {status}")
    
    print(f"\n{'='*80}")
    print("Generating summary report...")
    
    # Generate summary
    summary_file = reporter.generate_summary_report(all_analyses)
    
    # Generate consolidated verbose report if requested
    if args.verbose:
        print("Generating consolidated verbose report...")
        consolidated_file = reporter.generate_consolidated_verbose_report(all_analyses)
    
    print(f"{'='*80}")
    print(f"\nVerification complete!")
    print(f"Summary report: {summary_file}")
    if args.verbose:
        print(f"Consolidated report: {consolidated_file}")
        print(f"Individual reports: {args.output}/task_*_verification.txt")
    print(f"\n{'='*80}")
    
    # Print quick summary to terminal
    status_counts = defaultdict(int)
    for analysis in all_analyses:
        status = analysis.get('summary', {}).get('status', 'UNKNOWN')
        status_counts[status] += 1
    
    print("\nQuick Summary:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status:20s}: {count:4d}")
    
    # Print problematic tasks directly to terminal as well
    silent_failures = [a for a in all_analyses 
                      if a.get('summary', {}).get('status') == 'SILENT_FAILURE']
    if silent_failures:
        print(f"\n⚠️  Silent failures detected: {len(silent_failures)} tasks")
        print("   See VERIFICATION_SUMMARY.txt for complete list")
    
    incomplete = [a for a in all_analyses 
                 if a.get('summary', {}).get('status') == 'INCOMPLETE']
    if incomplete:
        print(f"\n⚠️  Incomplete tasks detected: {len(incomplete)} tasks")
        print("   See VERIFICATION_SUMMARY.txt for complete list")
    
    # Warning if ZAID not specified
    if not args.zaid:
        print(f"\n{'='*80}")
        print("⚠️  NOTE: --zaid was not specified.")
        print("   For accurate verification of tkemodel.dat and spinscalingmodel.dat,")
        print("   re-run with --zaid <compound_nucleus_zaid>")
        print(f"{'='*80}")


if __name__ == '__main__':
    main()

