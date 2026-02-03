"""
CGMF deformations.dat Parser and Writer

Handles ground-state quadrupole deformation parameters (beta2) from FRDM95 calculations.
Each line contains nuclear structure data with beta2 at fixed position 44.

File Format:
    Fixed-width columns: Z(4) A(4) El(3) fl(2) Mexp(10) Mth(10) Emic(10) beta2(8) beta3 beta4 beta6
    Beta2 extraction logic matches CGMF C++ implementation:
        - Skip to position 44 (beta2 column start)
        - Check position 6 of remaining substring
        - If not space, parse float; otherwise beta2 is missing

CRITICAL ZAID INTERPRETATION:
    - This file uses DIRECT nucleus identification (Z, A)
    - ZAID = Z * 1000 + A (no compound nucleus conversion)
    - Example: Oxygen-16 -> ZAID = 8016

Scaling Strategy:
    - Similar to kcksyst.dat: classification-based scaling
    - Each nucleus classified as STABLE or UNSTABLE
    - Two scaling factors: STAB_beta2 and UNSTAB_beta2
    - Applied based on stability criteria
"""

from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional


# ============================================================================
# FORMAT SPECIFICATIONS
# ============================================================================

# Column positions (0-indexed, matching CGMF C++ substr calls)
DEFORMATIONS_FORMATS = {
    'columns': {
        'Z': (0, 4),
        'A': (4, 4),
        'beta2_start': 44,
        'beta2_width': 7,
        'beta2_check_offset': 6,  # Position to check for data presence
    },
    'default_beta2': 0.2,  # CGMF default when missing
}

# Stability classification parameters
STABILITY_PARAMS = {
    'z_offset': 2.0,
    'z_fraction': 0.05,
}


# ============================================================================
# STABILITY CLASSIFICATION (matches kcksyst.dat exactly)
# ============================================================================

def _calculate_z_stable(a: int) -> float:
    """
    Calculate empirical stable-Z prediction for mass number A.
    
    Formula: Z_stable = A / (2 + 0.015 * A^(2/3))
    
    Args:
        a: Mass number (number of nucleons)
        
    Returns:
        Predicted proton number at valley of stability
    """
    return a / (2.0 + 0.015 * (a ** (2.0 / 3.0)))


def _is_stable_nucleus(z: int, a: int) -> bool:
    """
    Classify nucleus as STABLE or UNSTABLE.
    
    STABLE if EITHER condition holds:
        - |Z - Z_stable| < 2
        - |Z - Z_stable| < 0.05 * Z_stable
    
    Args:
        z: Atomic number (proton number)
        a: Mass number (nucleon number)
        
    Returns:
        True if STABLE, False if UNSTABLE
    """
    z_stable = _calculate_z_stable(a)
    delta_z = abs(z - z_stable)
    return (delta_z < STABILITY_PARAMS['z_offset']) or (delta_z < STABILITY_PARAMS['z_fraction'] * z_stable)


# ============================================================================
# PARSER
# ============================================================================

def parse(
    filepath: Path,
    target_zaid: int = 92235,
    preserve_format: bool = True
) -> Tuple[Dict[str, float], Optional[Dict[str, Any]]]:
    """
    Parse deformations.dat file.
    
    File contains beta2 deformation parameters for all nuclei in FRDM95 database.
    Uses fixed-width format with beta2 at position 44.
    
    CRITICAL PARSING STRATEGY (matches CGMF C++ implementation):
    - Z and A extracted from positions [0:4] and [4:8]
    - Beta2 column starts at position 44
    - Presence check: if character at position 50 (44+6) is not space, data exists
    - Beta2 value parsed from 8-character field starting at position 44
    
    STABILITY CLASSIFICATION:
    - Each nucleus (Z, A) classified as STABLE or UNSTABLE
    - Z_stable = A / (2 + 0.015 * A^(2/3))
    - STABLE if: |Z - Z_stable| < 2  OR  |Z - Z_stable| < 0.05 * Z_stable
    - UNSTABLE otherwise
    
    NOTE: This file contains systematics for ALL nuclei, not target-specific data.
          The target_zaid parameter is included for API consistency but not used.
          All nuclei are parsed and stored in format_info.
    
    NOTE: Scaling factors in this file are applied based on STABILITY, not ZAID:
          - 2 total scaling factors: STAB_beta2 and UNSTAB_beta2
    
    Args:
        filepath: Path to deformations.dat
        target_zaid: ZAID (not used, included for API consistency, default 92235)
        preserve_format: If True, return format information for reconstruction
        
    Returns:
        If preserve_format is False: Dictionary with 2 scaling factor keys (both 1.0)
        If preserve_format is True: Tuple of (params dict, format dict with file structure)
    """
    print(f"[PARSE] Reading file: {filepath}")
    print(f"[PARSE] Target ZAID: {target_zaid} (not used - file contains all nuclei)")
    print(f"[PARSE] Using FIXED-WIDTH parsing: beta2 at position 44")
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    params = {}
    format_info = {} if preserve_format else None
    
    # Store all file lines for format preservation
    header_lines = []
    data_lines = {}  # ZAID -> line info
    data_order = []  # Preserve original order
    footer_lines = []
    
    line_count = 0
    parsed_count = 0
    data_started = False
    data_ended = False
    
    # Statistics tracking
    stable_with_beta2 = 0
    unstable_with_beta2 = 0
    missing_beta2 = 0
    
    with open(filepath, 'r') as f:
        content = f.read()
        has_trailing_newline = content.endswith('\n')
        lines = content.splitlines()
    
    print(f"[PARSE] File has {len(lines)} lines")
    print(f"[PARSE] File has trailing newline: {has_trailing_newline}")
    
    column_specs = DEFORMATIONS_FORMATS['columns']
    z_start, z_width = column_specs['Z']
    a_start, a_width = column_specs['A']
    beta2_start = column_specs['beta2_start']
    beta2_width = column_specs['beta2_width']
    beta2_check_offset = column_specs['beta2_check_offset']
    
    for line_num, line in enumerate(lines, 1):
        line_count += 1
        
        # Handle header (before data starts)
        if not data_started:
            if line.strip().startswith('#') or not line.strip():
                header_lines.append(line)
                print(f"[PARSE] Line {line_num}: Header/comment")
                continue
            # First non-comment, non-empty line starts data
            data_started = True
        
        # Handle footer (after data ends)
        if data_started and data_ended:
            footer_lines.append(line)
            print(f"[PARSE] Line {line_num}: Footer")
            continue
        
        # Empty line signals end of data section
        if data_started and not line.strip():
            data_ended = True
            footer_lines.append(line)
            print(f"[PARSE] Line {line_num}: Empty line (data section ended)")
            continue
        
        # Try to parse as data line using FIXED-WIDTH extraction
        try:
            # Extract Z and A from fixed positions
            z = int(line[z_start:z_start + z_width].strip())
            a = int(line[a_start:a_start + a_width].strip())
            
            # Check for end of data (Z > 99)
            if z > 99:
                data_ended = True
                footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Z > 99 (data section ended)")
                continue
            
            # Calculate ZAID
            zaid = z * 1000 + a
            
            # Extract beta2 using CGMF logic
            # CRITICAL: Matches C++ code: str = str.substr(44); if (str[6]!=' ')
            beta2 = None
            if len(line) >= beta2_start + beta2_check_offset + 1:
                beta2_substring = line[beta2_start:]
                if beta2_substring[beta2_check_offset] != ' ':
                    # Parse the float (matches: atof(str.c_str()))
                    try:
                        beta2_str = beta2_substring[:beta2_width].strip()
                        beta2 = float(beta2_str)
                    except ValueError:
                        print(f"[PARSE] WARNING: Line {line_num}: Could not parse beta2 from '{beta2_substring[:beta2_width]}'")
                        beta2 = None
            
            # Classify as STABLE or UNSTABLE
            is_stable = _is_stable_nucleus(z, a)
            stability_label = "STABLE" if is_stable else "UNSTABLE"
            
            # Update statistics
            if beta2 is not None:
                if is_stable:
                    stable_with_beta2 += 1
                else:
                    unstable_with_beta2 += 1
            else:
                missing_beta2 += 1
            
            # Store in data_lines for format preservation
            isotope_key = zaid
            if preserve_format:
                data_lines[isotope_key] = {
                    'original_line': line,
                    'z': z,
                    'a': a,
                    'is_stable': is_stable,
                    'beta2': beta2,
                    'line_num': line_num,
                    'order': parsed_count
                }
                data_order.append(isotope_key)
            
            parsed_count += 1
            
            if beta2 is not None:
                print(f"[PARSE] Line {line_num}: Z={z:4d}, A={a:4d}, ZAID={zaid:6d} -> {stability_label} (beta2={beta2:8.3f})")
            else:
                print(f"[PARSE] Line {line_num}: Z={z:4d}, A={a:4d}, ZAID={zaid:6d} -> {stability_label} (beta2=MISSING)")
            
        except (ValueError, IndexError) as e:
            # Failed to parse as data
            if data_started:
                data_ended = True
                footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Could not parse as data (footer?): {e}")
            else:
                header_lines.append(line)
                print(f"[PARSE] Line {line_num}: Could not parse as data (header?): {e}")
    
    print(f"[PARSE] Summary: Read {line_count} lines, parsed {parsed_count} nuclei")
    print(f"[PARSE] Header lines: {len(header_lines)}")
    print(f"[PARSE] Data entries: {len(data_lines)}")
    print(f"[PARSE] Footer lines: {len(footer_lines)}")
    
    # Initialize 2 scaling factors to 1.0
    # These are the "parameters" for this file (not actual nuclear data)
    params['STAB_beta2'] = 1.0
    params['UNSTAB_beta2'] = 1.0
    
    print(f"[PARSE] Initialized {len(params)} scaling factors (2 total: STAB_beta2 + UNSTAB_beta2)")
    
    if preserve_format:
        format_info = {
            'header_lines': header_lines,
            'data_lines': data_lines,
            'data_order': data_order,
            'footer_lines': footer_lines,
            'target_zaid': target_zaid,  # Store for consistency (not used)
            '_metadata': {
                'has_trailing_newline': has_trailing_newline,
                'total_nuclei': parsed_count,
                'stable_with_beta2': stable_with_beta2,
                'unstable_with_beta2': unstable_with_beta2,
                'missing_beta2': missing_beta2,
                'all_nuclei': sorted(data_lines.keys())
            }
        }
        print(f"[PARSE] Beta2 statistics: STABLE={stable_with_beta2}, UNSTABLE={unstable_with_beta2}, MISSING={missing_beta2}")
        return params, format_info
    else:
        return params, None


def write(
    filepath: Path,
    params: Dict[str, float],
    format_info: Optional[Dict[str, Any]] = None,
    target_zaid: int = 92235
) -> None:
    """
    Write parameters to deformations.dat file.
    
    CRITICAL FORMATTING REQUIREMENTS (matches CGMF C++ implementation):
    - Beta2 field starts at position 44
    - Beta2 field is 7 characters wide (right-aligned, 3 decimals)
    - All other columns preserved exactly from original
    
    Format preservation:
    - If format_info provided: preserves entire file structure, applying scaling factors
    - If format_info is None: raises error (cannot reconstruct file without format info)
    
    NOTE: Scaling factors are applied based on STABILITY classification:
          - Each nucleus gets beta2 scaled by either STAB_beta2 or UNSTAB_beta2
          - STABLE nuclei:   scale by STAB_beta2
          - UNSTABLE nuclei: scale by UNSTAB_beta2
    
    Args:
        filepath: Path to write deformations.dat
        params: Dictionary with 2 scaling factors (STAB_beta2 and UNSTAB_beta2)
        format_info: Format information from parse (REQUIRED)
        target_zaid: ZAID (not used, included for API consistency, default 92235)
    """
    print(f"[WRITE] Writing to file: {filepath}")
    print(f"[WRITE] Target ZAID: {target_zaid} (not used - file contains all nuclei)")
    print(f"[WRITE] Using FIXED-WIDTH formatting: beta2 at position 44, width 7")
    
    if format_info is None:
        raise ValueError("format_info is required for writing deformations.dat - cannot reconstruct file structure without it")
    
    # Validate required parameters (2 scaling factors)
    if 'STAB_beta2' not in params:
        raise ValueError("params must contain 'STAB_beta2' scaling factor")
    if 'UNSTAB_beta2' not in params:
        raise ValueError("params must contain 'UNSTAB_beta2' scaling factor")
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract format info
    header_lines = format_info['header_lines']
    data_lines = format_info['data_lines']
    data_order = format_info['data_order']
    footer_lines = format_info['footer_lines']
    has_trailing_newline = format_info['_metadata']['has_trailing_newline']
    
    stab_beta2_scale = params['STAB_beta2']
    unstab_beta2_scale = params['UNSTAB_beta2']
    
    print(f"[WRITE] Reconstructing file with {len(header_lines)} header lines, {len(data_lines)} data entries, {len(footer_lines)} footer lines")
    print(f"[WRITE] Using ORIGINAL ORDER (not sorted): {len(data_order)} nuclei")
    print(f"[WRITE] Scale factors: STAB_beta2={stab_beta2_scale}, UNSTAB_beta2={unstab_beta2_scale}")
    
    column_specs = DEFORMATIONS_FORMATS['columns']
    beta2_start = column_specs['beta2_start']
    beta2_width = column_specs['beta2_width']
    
    lines = []
    
    # Write header
    lines.extend(header_lines)
    
    # Write data section IN ORIGINAL ORDER
    modified_count = 0
    unchanged_count = 0
    
    for zaid in data_order:
        entry = data_lines[zaid]
        z = entry['z']
        a = entry['a']
        is_stable = entry['is_stable']
        orig_beta2 = entry['beta2']
        
        # If no beta2 data, preserve original line
        if orig_beta2 is None:
            line = entry['original_line']
            lines.append(line)
            unchanged_count += 1
            continue
        
        # Determine which scaling factor to use
        if is_stable:
            scale_factor = stab_beta2_scale
            prefix = 'STAB'
        else:
            scale_factor = unstab_beta2_scale
            prefix = 'UNSTAB'
        
        # Apply scaling factor
        scaled_beta2 = orig_beta2 * scale_factor
        
        # Check if value changed (accounting for floating point precision)
        value_unchanged = abs(scaled_beta2 - orig_beta2) < 1e-15
        
        if value_unchanged:
            # Unchanged - preserve original line exactly
            line = entry['original_line']
            unchanged_count += 1
            print(f"[WRITE] ZAID={zaid:6d}: Unchanged (preserving original)")
        else:
            # Modified - reconstruct line with scaled beta2
            original_line = entry['original_line']
            
            # Keep everything before beta2 column
            line_prefix = original_line[:beta2_start]
            
            # Format new beta2 value (7 chars, 3 decimals, right-aligned)
            beta2_formatted = f"{scaled_beta2:7.3f}"
            
            # Keep everything after beta2 column
            beta2_end = beta2_start + beta2_width
            if len(original_line) > beta2_end:
                line_suffix = original_line[beta2_end:]
            else:
                line_suffix = ''
            
            # Combine parts
            line = line_prefix + beta2_formatted + line_suffix
            
            modified_count += 1
            stability_label = "STABLE" if is_stable else "UNSTABLE"
            print(f"[WRITE] ZAID={zaid:6d} ({stability_label}): Modified")
            print(f"[WRITE]   beta2: {orig_beta2:8.3f} -> {scaled_beta2:8.3f} (scale={scale_factor})")
        
        lines.append(line)
    
    # Write footer
    lines.extend(footer_lines)
    
    # Write to file
    with open(filepath, 'w') as f:
        for i, line in enumerate(lines):
            f.write(line)
            if i < len(lines) - 1:
                f.write('\n')
            elif has_trailing_newline:
                f.write('\n')
    
    print(f"[WRITE] Successfully wrote {len(data_lines)} data entries to {filepath}")
    print(f"[WRITE] Modified {modified_count} entries, preserved {unchanged_count} unchanged")
    print(f"[WRITE] Trailing newline: {has_trailing_newline}")
