from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

def parse(filepath: Path, target_zaid: int = 92235, preserve_format: bool = True) -> Tuple[Dict[str, float], Optional[Dict[str, Any]]]:
    """
    Parse spinscalingmodel.dat file.
    
    Format: Stream-based whitespace-delimited tokens
    ZAID    alpha_0    alpha_slope    # comment
    
    NOTE: ZAID interpretation in this file:
          - ZAIDs in file represent COMPOUND nucleus (target + neutron)
          - For neutron-induced fission: compound_zaid = target_zaid + 1
            Example: target U-235 (92235) -> file has 92236
          - Negative ZAIDs: Spontaneous fission (no conversion needed)
            Example: -98252 = Cf-252(sf)
          - C++ stores ZAID as double, but we handle as int for clarity
    
    Args:
        filepath: Path to spinscalingmodel.dat
        target_zaid: ZAID of target nucleus for neutron-induced (e.g., 92235 for U-235(n,f))
                     or negative ZAID for spontaneous fission (e.g., -98252 for Cf-252(sf))
        preserve_format: If True, also return format information for exact reconstruction
        
    Returns:
        If preserve_format is False: Dictionary with 'alpha_0' and 'alpha_slope'
        If preserve_format is True: Tuple of (params dict, format dict with file structure)
    """
    print(f"[PARSE] Reading file: {filepath}")
    print(f"[PARSE] Target ZAID: {target_zaid}")
    
    # Convert target ZAID to compound ZAID for file lookup
    if target_zaid > 0:
        compound_zaid = target_zaid + 1
        print(f"[PARSE] Compound ZAID (for file lookup): {compound_zaid} (target + neutron)")
    else:
        compound_zaid = target_zaid
        print(f"[PARSE] Spontaneous fission ZAID: {compound_zaid} (no conversion)")
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    params = {}
    format_info = {} if preserve_format else None
    
    # Store all file lines for format preservation
    all_lines = []
    header_lines = []
    data_lines = {}  # ZAID -> line info with ORDER
    data_order = []  # Preserve the original order of ZAIDs
    footer_lines = []
    
    line_count = 0
    parsed_count = 0
    in_data_section = False
    found_header = False
    data_section_ended = False
    target_found = False
    
    with open(filepath, 'r') as f:
        content = f.read()
        has_trailing_newline = content.endswith('\n')
        lines = content.splitlines()
    
    print(f"[PARSE] File has {len(lines)} lines")
    print(f"[PARSE] File has trailing newline: {has_trailing_newline}")
    
    for line_num, line in enumerate(lines, 1):
        line_count += 1
        all_lines.append(line)
        
        # Detect start of data section (line with "ZAID" header)
        if 'ZAID' in line and ('alpha_0' in line or 'alpha' in line):
            print(f"[PARSE] Line {line_num}: Found data header")
            found_header = True
            header_lines.append(line)
            in_data_section = True
            continue
        
        # Before data section starts
        if not found_header:
            header_lines.append(line)
            if line.strip() and not line.strip().startswith('#'):
                print(f"[PARSE] Line {line_num}: Header/comment line")
            continue
        
        # Handle empty lines (potential end of data section)
        if not line.strip():
            if parsed_count > 0 and not data_section_ended:
                data_section_ended = True
                print(f"[PARSE] Line {line_num}: Empty line - data section ended")
            footer_lines.append(line)
            continue
        
        # Check if this is a comment line (footer)
        stripped = line.strip()
        if stripped.startswith('#'):
            if parsed_count > 0:
                data_section_ended = True
            footer_lines.append(line)
            print(f"[PARSE] Line {line_num}: Comment/footer")
            continue
        
        # Try to parse as data line using stream-based tokenization
        if in_data_section and not data_section_ended:
            try:
                # Split on whitespace, remove inline comments
                comment = ''
                if '#' in line:
                    parts = line.split('#')
                    tokens = parts[0].split()
                    comment = '#' + parts[1]
                else:
                    tokens = line.split()
                
                # Must have exactly 3 tokens: ZAID, alpha_0, alpha_slope
                if len(tokens) == 3:
                    # C++ stores ZAID as double, but we parse as int
                    zaid = int(float(tokens[0]))
                    alpha_0 = float(tokens[1])
                    alpha_slope = float(tokens[2])
                    
                    # Store in data_lines for format preservation
                    if preserve_format:
                        data_lines[zaid] = {
                            'original_line': line,
                            'alpha_0': alpha_0,
                            'alpha_slope': alpha_slope,
                            'comment': comment,
                            'line_num': line_num,
                            'order': parsed_count
                        }
                        data_order.append(zaid)
                    
                    parsed_count += 1
                    print(f"[PARSE] Line {line_num}: ZAID={zaid}, alpha_0={alpha_0}, alpha_slope={alpha_slope}")
                    
                    # Check if this is our compound ZAID
                    if zaid == compound_zaid:
                        params['alpha_0'] = alpha_0
                        params['alpha_slope'] = alpha_slope
                        target_found = True
                        print(f"[PARSE] Line {line_num}: *** COMPOUND ZAID FOUND (target={target_zaid}) ***")
                else:
                    # Insufficient columns - treat as footer
                    data_section_ended = True
                    footer_lines.append(line)
                    print(f"[PARSE] Line {line_num}: Insufficient columns ({len(tokens)}), treating as footer")
                    
            except (ValueError, IndexError) as e:
                # Parsing failed - treat as footer
                data_section_ended = True
                footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Could not parse as data (footer?): {e}")
        else:
            footer_lines.append(line)
    
    print(f"[PARSE] Summary: Read {line_count} lines, parsed {parsed_count} isotope entries")
    print(f"[PARSE] Header lines: {len(header_lines)}")
    print(f"[PARSE] Data entries: {len(data_lines)}")
    print(f"[PARSE] Footer lines: {len(footer_lines)}")
    
    if not target_found:
        raise ValueError(f"Target ZAID {target_zaid} (compound ZAID {compound_zaid}) not found in file. Available ZAIDs: {sorted(data_lines.keys())}")
    
    print(f"[PARSE] Target parameters: alpha_0={params['alpha_0']}, alpha_slope={params['alpha_slope']}")
    
    if preserve_format:
        format_info = {
            'header_lines': header_lines,
            'data_lines': data_lines,
            'data_order': data_order,
            'footer_lines': footer_lines,
            'target_zaid': target_zaid,  # Store original target ZAID
            'compound_zaid': compound_zaid,  # Store compound ZAID for writing
            '_metadata': {
                'has_trailing_newline': has_trailing_newline,
                'all_zaids': sorted(data_lines.keys())
            }
        }
        return params, format_info
    else:
        return params, None


def write(filepath: Path, params: Dict[str, float],
                     format_info: Optional[Dict[str, Any]] = None,
                     target_zaid: int = 92235,
                     alpha_0_scale: float = 1.0,
                     alpha_slope_scale: float = 1.0) -> None:
    """
    Write parameters to spinscalingmodel.dat file.
    
    Format preservation:
    - If format_info provided: preserves entire file structure, modifying only target ZAID line
    - If format_info is None: raises error (cannot reconstruct file without format info)
    
    NOTE: Each parameter (alpha_0, alpha_slope) is scaled independently.
    
    NOTE: ZAID interpretation in this file:
          - ZAIDs in file represent COMPOUND nucleus (target + neutron)
          - For neutron-induced fission: compound_zaid = target_zaid + 1
            Example: target U-235 (92235) -> file has 92236
          - Negative ZAIDs: Spontaneous fission (no conversion)
            Example: -98252 = Cf-252(sf)
    
    Args:
        filepath: Path to write spinscalingmodel.dat
        params: Dictionary with 'alpha_0' and 'alpha_slope'
        format_info: Format information from parse_spinscaling (REQUIRED)
        target_zaid: ZAID of target nucleus being modified
        alpha_0_scale: Multiplicative scaling factor for alpha_0 (default 1.0)
        alpha_slope_scale: Multiplicative scaling factor for alpha_slope (default 1.0)
    """
    print(f"[WRITE] Writing to file: {filepath}")
    print(f"[WRITE] Target ZAID: {target_zaid}")
    print(f"[WRITE] Scale factors: alpha_0={alpha_0_scale}, alpha_slope={alpha_slope_scale}")
    
    if format_info is None:
        raise ValueError("format_info is required for writing spinscalingmodel.dat - cannot reconstruct file structure without it")
    
    # Validate required parameters
    if 'alpha_0' not in params or 'alpha_slope' not in params:
        raise ValueError("params must contain 'alpha_0' and 'alpha_slope'")
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract format info
    header_lines = format_info['header_lines']
    data_lines = format_info['data_lines']
    data_order = format_info['data_order']
    footer_lines = format_info['footer_lines']
    compound_zaid = format_info.get('compound_zaid')
    has_trailing_newline = format_info['_metadata']['has_trailing_newline']
    
    # Calculate compound ZAID if not stored in format_info
    if compound_zaid is None:
        if target_zaid > 0:
            compound_zaid = target_zaid + 1
        else:
            compound_zaid = target_zaid
    
    print(f"[WRITE] Compound ZAID (in file): {compound_zaid}")
    print(f"[WRITE] Reconstructing file with {len(header_lines)} header lines, {len(data_lines)} data entries, {len(footer_lines)} footer lines")
    print(f"[WRITE] Using ORIGINAL ORDER (not sorted): {data_order}")
    
    if compound_zaid not in data_lines:
        raise ValueError(f"Cannot write: target ZAID {target_zaid} (compound ZAID {compound_zaid}) not found in original file. Available: {sorted(data_lines.keys())}")
    
    lines = []
    
    # Write header
    lines.extend(header_lines)
    
    # Define format specifications for consistent spacing
    SPINSCALING_FORMATS = {
        'zaid': '{:>6}',        # Width 6, handles negative SF (e.g., -98252)
        'alpha_0': '{:>5.2f}',  # Width 5, 2 decimals
        'alpha_slope': '{:>6.3f}'  # Width 6, 3 decimals
    }
    
    # Write data section IN ORIGINAL ORDER (not sorted!)
    modified_count = 0
    for zaid in data_order:
        entry = data_lines[zaid]
        
        if zaid == compound_zaid:
            # Apply scaling factors to each parameter independently
            scaled_alpha_0 = params['alpha_0'] * alpha_0_scale
            scaled_alpha_slope = params['alpha_slope'] * alpha_slope_scale
            
            # Check if values changed
            original_alpha_0 = entry['alpha_0']
            original_alpha_slope = entry['alpha_slope']
            
            if (abs(scaled_alpha_0 - original_alpha_0) < 1e-15 and
                abs(scaled_alpha_slope - original_alpha_slope) < 1e-15):
                # Unchanged - use original line
                line = entry['original_line']
                print(f"[WRITE] ZAID {zaid} (target {target_zaid}): Unchanged (preserving original)")
            else:
                # Modified - reconstruct line with proper spacing
                comment = entry['comment']
                
                # Format with explicit spacing to prevent token merging
                zaid_str = SPINSCALING_FORMATS['zaid'].format(zaid)
                alpha_0_str = SPINSCALING_FORMATS['alpha_0'].format(scaled_alpha_0)
                alpha_slope_str = SPINSCALING_FORMATS['alpha_slope'].format(scaled_alpha_slope)
                
                # Explicit space separators ensure stream-based parsing works
                line = f"{zaid_str} {alpha_0_str} {alpha_slope_str}"
                if comment:
                    line += f" {comment}"
                
                modified_count += 1
                print(f"[WRITE] ZAID {zaid} (target {target_zaid}): Modified")
                print(f"[WRITE]   alpha_0: {original_alpha_0:.2f} -> {scaled_alpha_0:.2f} (scale={alpha_0_scale})")
                print(f"[WRITE]   alpha_slope: {original_alpha_slope:.3f} -> {scaled_alpha_slope:.3f} (scale={alpha_slope_scale})")
        else:
            # Other ZAID - preserve original
            line = entry['original_line']
        
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
    print(f"[WRITE] Modified {modified_count} entries")
    print(f"[WRITE] Trailing newline: {has_trailing_newline}")
