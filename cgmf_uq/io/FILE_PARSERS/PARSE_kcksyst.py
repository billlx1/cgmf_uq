from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

# Format specifications matching CGMF C++ parser requirements
# Each data line is EXACTLY 97 characters
KCKSYST_FORMATS = {
    # Field positions as (start, width) matching C++ substr() calls
    'columns': {
        'Z':      (0,  5),
        'A':      (5,  6),
        'Pairing': (11, 13),
        'Eshell':  (24, 13),
        'Ematch':  (37, 10),
        'astar':   (47, 10),
        'T':       (57, 10),
        'E0':      (67, 10),
        'Tsys':    (77, 10),
        'E0sys':   (87, 10),
    },
    # Python format strings for writing (total: 97 chars)
    'line_format': (
        "{:5d}"       # Z        [0:5]
        "{:6d}"       # A        [5:11]
        "{:>13.5e}"   # Pairing  [11:24]
        "{:>13.5e}"   # Eshell   [24:37]
        "{:>10.5f}"   # Ematch   [37:47]
        "{:>10.5f}"   # astar    [47:57]
        "{:>10.5f}"   # T        [57:67]
        "{:>10.5f}"   # E0       [67:77]
        "{:>10.5f}"   # Tsys     [77:87]
        "{:>10.5f}"   # E0sys    [87:97]
    ),
    'param_names': ['Pairing', 'Eshell', 'Ematch', 'astar', 'T', 'E0', 'Tsys', 'E0sys']
}


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
    return (delta_z < 2.0) or (delta_z < 0.05 * z_stable)


def parse(filepath: Path, target_zaid: int = 92235, preserve_format: bool = True) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    Parse kcksyst.dat file (level density systematics).
    
    CRITICAL PARSING STRATEGY (matches CGMF C++ implementation):
    - File is STRICTLY fixed-width: every data line is EXACTLY 97 characters
    - Field positions defined by C++ substr() calls and must be matched exactly:
      * Z:       [0:5]    (5 chars)
      * A:       [5:11]   (6 chars)
      * Pairing: [11:24]  (13 chars, scientific notation)
      * Eshell:  [24:37]  (13 chars, scientific notation)
      * Ematch:  [37:47]  (10 chars, float)
      * astar:   [47:57]  (10 chars, float)
      * T:       [57:67]  (10 chars, float)
      * E0:      [67:77]  (10 chars, float)
      * Tsys:    [77:87]  (10 chars, float)
      * E0sys:   [87:97]  (10 chars, float)
    
    C++ CONDITIONAL READ BEHAVIOR:
    - All fields up through astar read unconditionally
    - Then branches on Ematch value:
      * if (Ematch > 0): reads T at [57:67] and E0 at [67:77]
      * else:            reads Tsys at [77:87] and E0sys at [87:97]
    - This parser reads ALL 10 columns unconditionally and stores faithfully
    
    STABILITY CLASSIFICATION:
    - Each nucleus (Z, A) classified as STABLE or UNSTABLE
    - Z_stable = A / (2 + 0.015 * A^(2/3))
    - STABLE if: |Z - Z_stable| < 2  OR  |Z - Z_stable| < 0.05 * Z_stable
    - UNSTABLE otherwise
    
    NOTE: This file contains systematics for ALL isotopes, not target-specific data.
          The target_zaid parameter is included for API consistency but not used.
          All isotopes are parsed and stored in format_info.
    
    NOTE: Scaling factors in this file are applied based on STABILITY, not ZAID:
          - 8 parameters per isotope: Pairing, Eshell, Ematch, astar, T, E0, Tsys, E0sys
          - 16 total scaling factors: STAB_* and UNSTAB_* versions of each parameter
    
    Format: Fixed-width, 97 characters per line
    Z(5) A(6) Pairing(13) Eshell(13) Ematch(10) astar(10) T(10) E0(10) Tsys(10) E0sys(10)
    
    Args:
        filepath: Path to kcksyst.dat
        target_zaid: ZAID (not used, included for API consistency, default 92235)
        preserve_format: If True, also return format information for exact reconstruction
        
    Returns:
        If preserve_format is False: Dictionary with 16 scaling factor keys (all 1.0)
        If preserve_format is True: Tuple of (params dict, format dict with file structure)
    """
    print(f"[PARSE] Reading file: {filepath}")
    print(f"[PARSE] Target ZAID: {target_zaid} (not used - file contains all isotopes)")
    print(f"[PARSE] Using FIXED-WIDTH parsing: 97-character lines")
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    params = {}
    format_info = {} if preserve_format else None
    
    # Store all file lines for format preservation
    header_lines = []
    data_lines = {}  # (Z, A) -> line info
    data_order = []  # Preserve original order as (Z, A) tuples
    footer_lines = []
    
    line_count = 0
    parsed_count = 0
    data_started = False
    data_ended = False
    
    with open(filepath, 'r') as f:
        content = f.read()
        has_trailing_newline = content.endswith('\n')
        lines = content.splitlines()
    
    print(f"[PARSE] File has {len(lines)} lines")
    print(f"[PARSE] File has trailing newline: {has_trailing_newline}")
    
    column_specs = KCKSYST_FORMATS['columns']
    param_names = KCKSYST_FORMATS['param_names']
    
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
            z = int(line[0:5])
            a = int(line[5:11])
            
            # Extract all 8 parameters from fixed positions
            param_values = {}
            for name in param_names:
                start, width = column_specs[name]
                param_values[name] = float(line[start:start + width])
            
            # Classify as STABLE or UNSTABLE
            is_stable = _is_stable_nucleus(z, a)
            stability_label = "STABLE" if is_stable else "UNSTABLE"
            
            # Extract comment if present (after position 97)
            comment = ''
            if len(line) > 97 and '#' in line[97:]:
                comment_start = line[97:].find('#')
                comment = line[97 + comment_start:]
            
            # Store in data_lines for format preservation
            isotope_key = (z, a)
            if preserve_format:
                data_lines[isotope_key] = {
                    'original_line': line,
                    'z': z,
                    'a': a,
                    'is_stable': is_stable,
                    'params': param_values,
                    'comment': comment,
                    'line_num': line_num,
                    'order': parsed_count
                }
                data_order.append(isotope_key)
            
            parsed_count += 1
            print(f"[PARSE] Line {line_num}: Z={z:4d}, A={a:4d} -> {stability_label} (Ematch={param_values['Ematch']:.5f})")
            
        except (ValueError, IndexError) as e:
            # Failed to parse as data
            if data_started:
                data_ended = True
                footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Could not parse as data (footer?): {e}")
            else:
                header_lines.append(line)
                print(f"[PARSE] Line {line_num}: Could not parse as data (header?): {e}")
    
    print(f"[PARSE] Summary: Read {line_count} lines, parsed {parsed_count} isotope entries")
    print(f"[PARSE] Header lines: {len(header_lines)}")
    print(f"[PARSE] Data entries: {len(data_lines)}")
    print(f"[PARSE] Footer lines: {len(footer_lines)}")
    
    # Initialize all 16 scaling factors to 1.0
    # These are the "parameters" for this file (not actual nuclear data)
    for prefix in ('STAB', 'UNSTAB'):
        for name in param_names:
            params[f"{prefix}_{name}"] = 1.0
    
    print(f"[PARSE] Initialized {len(params)} scaling factors (16 total: 8 STAB + 8 UNSTAB)")
    
    if preserve_format:
        stable_count = sum(1 for entry in data_lines.values() if entry['is_stable'])
        unstable_count = parsed_count - stable_count
        
        format_info = {
            'header_lines': header_lines,
            'data_lines': data_lines,
            'data_order': data_order,
            'footer_lines': footer_lines,
            'target_zaid': target_zaid,  # Store for consistency (not used)
            '_metadata': {
                'has_trailing_newline': has_trailing_newline,
                'total_isotopes': parsed_count,
                'stable_count': stable_count,
                'unstable_count': unstable_count,
                'all_isotopes': sorted(data_lines.keys())
            }
        }
        print(f"[PARSE] Stability counts: STABLE={stable_count}, UNSTABLE={unstable_count}")
        return params, format_info
    else:
        return params, None


def write(filepath: Path, params: Dict[str, float],
          format_info: Optional[Dict[str, Any]] = None,
          target_zaid: int = 92235,
          scale_factors: Optional[Dict[str, float]] = None) -> None:
    """
    Write parameters to kcksyst.dat file.
    
    CRITICAL FORMATTING REQUIREMENTS (matches CGMF C++ implementation):
    - Every data line MUST be exactly 97 characters
    - Field positions MUST match C++ substr() calls exactly
    - All fields right-aligned within their fixed widths
    
    Format preservation:
    - If format_info provided: preserves entire file structure, applying scaling factors
    - If format_info is None: raises error (cannot reconstruct file without format info)
    
    NOTE: Scaling factors are applied based on STABILITY classification:
          - Each isotope gets 8 parameters scaled by either STAB_* or UNSTAB_* factors
          - STABLE isotopes:   scale by STAB_Pairing, STAB_Eshell, etc.
          - UNSTABLE isotopes: scale by UNSTAB_Pairing, UNSTAB_Eshell, etc.
    
    Args:
        filepath: Path to write kcksyst.dat
        params: Dictionary with 16 scaling factors (STAB_* and UNSTAB_* versions)
        format_info: Format information from parse (REQUIRED)
        target_zaid: ZAID (not used, included for API consistency, default 92235)
        scale_factors: Dict with 16 keys (STAB_Pairing, UNSTAB_Pairing, etc.)
                      If None, uses values from params dict (all should be 1.0)
    """
    print(f"[WRITE] Writing to file: {filepath}")
    print(f"[WRITE] Target ZAID: {target_zaid} (not used - file contains all isotopes)")
    print(f"[WRITE] Using FIXED-WIDTH formatting: 97-character lines")
    
    if format_info is None:
        raise ValueError("format_info is required for writing kcksyst.dat - cannot reconstruct file structure without it")
    
    # Build effective scaling factors
    # Priority: scale_factors argument > params dict (which should be all 1.0 from parse)
    param_names = KCKSYST_FORMATS['param_names']
    effective_scales = {}
    
    for prefix in ('STAB', 'UNSTAB'):
        for name in param_names:
            key = f"{prefix}_{name}"
            if scale_factors is not None and key in scale_factors:
                effective_scales[key] = scale_factors[key]
            elif key in params:
                effective_scales[key] = params[key]
            else:
                raise ValueError(f"Missing scaling factor '{key}' in both scale_factors and params")
    
    print(f"[WRITE] Applied {len(effective_scales)} scaling factors (16 total: 8 STAB + 8 UNSTAB)")
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract format info
    header_lines = format_info['header_lines']
    data_lines = format_info['data_lines']
    data_order = format_info['data_order']
    footer_lines = format_info['footer_lines']
    has_trailing_newline = format_info['_metadata']['has_trailing_newline']
    
    print(f"[WRITE] Reconstructing file with {len(header_lines)} header lines, {len(data_lines)} data entries, {len(footer_lines)} footer lines")
    print(f"[WRITE] Using ORIGINAL ORDER (not sorted): {len(data_order)} isotopes")
    
    lines = []
    
    # Write header
    lines.extend(header_lines)
    
    # Get format string
    line_format = KCKSYST_FORMATS['line_format']
    
    # Write data section IN ORIGINAL ORDER
    modified_count = 0
    unchanged_count = 0
    
    for isotope_key in data_order:
        entry = data_lines[isotope_key]
        z = entry['z']
        a = entry['a']
        is_stable = entry['is_stable']
        orig_params = entry['params']
        
        # Determine which scaling factor prefix to use
        prefix = 'STAB' if is_stable else 'UNSTAB'
        
        # Apply scaling factors to each parameter
        scaled_params = {}
        for name in param_names:
            scale_key = f"{prefix}_{name}"
            scale_value = effective_scales.get(scale_key, 1.0)
            scaled_params[name] = orig_params[name] * scale_value
        
        # Check if values changed (accounting for floating point precision)
        values_unchanged = all(
            abs(scaled_params[name] - orig_params[name]) < 1e-15
            for name in param_names
        )
        
        if values_unchanged:
            # Unchanged - preserve original line exactly
            line = entry['original_line']
            unchanged_count += 1
            print(f"[WRITE] Z={z:4d}, A={a:4d}: Unchanged (preserving original)")
        else:
            # Modified - reconstruct line with EXACT formatting (97 chars)
            line = line_format.format(
                z, a,
                scaled_params['Pairing'],
                scaled_params['Eshell'],
                scaled_params['Ematch'],
                scaled_params['astar'],
                scaled_params['T'],
                scaled_params['E0'],
                scaled_params['Tsys'],
                scaled_params['E0sys'],
            )
            
            # CRITICAL ASSERTION: Verify line is exactly 97 characters
            assert len(line) == 97, f"kcksyst line must be exactly 97 chars, got {len(line)}: '{line}'"
            
            # Add comment if original had one
            if entry['comment']:
                line += entry['comment']
            
            modified_count += 1
            stability_label = "STABLE" if is_stable else "UNSTABLE"
            print(f"[WRITE] Z={z:4d}, A={a:4d}: Modified ({stability_label})")
            print(f"[WRITE]   Scaled by {prefix}_* factors")
            print(f"[WRITE]   First 3 params: Pairing={scaled_params['Pairing']:.5e}, Eshell={scaled_params['Eshell']:.5e}, Ematch={scaled_params['Ematch']:.5f}")
        
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
