from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

# Format specifications matching CGMF C++ parser requirements
RTA_FORMATS = {
    'zaid': '{:^7}',     # Width 7, right-aligned (handles negative SF)
    'amin': '{:^5}',     # Width 5, right-aligned
    'amax': '{:^5}',     # Width 6, right-aligned
    'data': '{:>6.3f}'   # Width 6, 3 decimals (data array elements)
}

def parse(filepath: Path, target_zaid: int = 92235, preserve_format: bool = True) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    Parse rta.dat file (R_T(A) data).
    
    CRITICAL PARSING STRATEGY (matches CGMF C++ implementation):
    - Header uses FIXED-WIDTH extraction at exact positions:
      * ZAID: characters [0:7] (7 chars)
      * Amin: characters [7:12] (5 chars)
      * Amax: characters [12:18] (6 chars)
    - Data array: WHITESPACE-DELIMITED starting at character index 18
    
    Format: ZAID Amin Amax R_T(A) values...
    - Each line contains data for one isotope
    - R_T(A) is an array of values from mass Amin to Amax
    
    NOTE: ZAID interpretation in this file:
          - Positive ZAID: Target nucleus for neutron-induced fission
            Example: 92235 = U-235(n,f)
          - Negative ZAID: Spontaneous fission
            Example: -98252 = Cf-252(sf)
    
    Args:
        filepath: Path to rta.dat
        target_zaid: ZAID of target nucleus (e.g., 92235 for U-235(n,f))
        preserve_format: If True, also return format information for exact reconstruction
        
    Returns:
        If preserve_format is False: Dictionary with 'amin', 'amax', 'rt_values'
        If preserve_format is True: Tuple of (params dict, format dict with file structure)
    """
    print(f"[PARSE] Reading file: {filepath}")
    print(f"[PARSE] Target ZAID: {target_zaid}")
    print(f"[PARSE] Using HYBRID parsing: fixed-width header + whitespace-delimited data")
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    params = {}
    format_info = {} if preserve_format else None
    
    # Store all file lines for format preservation
    header_lines = []
    data_lines = {}  # ZAID -> line info
    data_order = []  # Preserve original order of ZAIDs
    footer_lines = []
    
    line_count = 0
    parsed_count = 0
    in_data_section = False
    target_found = False
    
    with open(filepath, 'r') as f:
        content = f.read()
        has_trailing_newline = content.endswith('\n')
        lines = content.splitlines()
    
    print(f"[PARSE] File has {len(lines)} lines")
    print(f"[PARSE] File has trailing newline: {has_trailing_newline}")
    
    for line_num, line in enumerate(lines, 1):
        line_count += 1
        
        # Skip empty lines at start (header)
        if not line.strip():
            if not in_data_section:
                header_lines.append(line)
                print(f"[PARSE] Line {line_num}: Empty line (header)")
            else:
                footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Empty line (footer)")
            continue
        
        # Skip comment lines
        if line.strip().startswith('#'):
            if not in_data_section or parsed_count == 0:
                header_lines.append(line)
                print(f"[PARSE] Line {line_num}: Comment (header)")
            else:
                footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Comment (footer)")
            continue
        
        # Try to parse as data line using FIXED-WIDTH header extraction
        try:
            # Check if line is long enough for header (need at least 18 chars)
            if len(line) < 18:
                # Too short for valid data line
                if not in_data_section or parsed_count == 0:
                    header_lines.append(line)
                else:
                    footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Too short for data line ({len(line)} chars)")
                continue
            
            # CRITICAL: Use fixed-width extraction to match C++ substr() behavior
            zaid_str = line[0:7].strip()
            amin_str = line[7:12].strip()
            amax_str = line[12:18].strip()
            
            # Validate that we got numeric values
            if not zaid_str or not amin_str or not amax_str:
                # One or more header fields empty
                if not in_data_section or parsed_count == 0:
                    header_lines.append(line)
                else:
                    footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Empty header field(s)")
                continue
            
            # Convert header values
            zaid = int(zaid_str)
            amin = int(amin_str)
            amax = int(amax_str)
            
            # Extract data array from position 18 onward (whitespace-delimited)
            data_str = line[18:].strip()
            rt_values = [float(x) for x in data_str.split()]
            
            # Verify array length matches expected range
            expected_length = amax - amin + 1
            if len(rt_values) != expected_length:
                print(f"[PARSE] WARNING: Line {line_num}: Expected {expected_length} values for A={amin}..{amax}, got {len(rt_values)}")
            
            # Extract comment if present (would be in data_str)
            comment = ''
            if '#' in line[18:]:
                comment_start = line[18:].find('#')
                comment = line[18 + comment_start:]
            
            # Store in data_lines for format preservation
            if preserve_format:
                data_lines[zaid] = {
                    'original_line': line,
                    'amin': amin,
                    'amax': amax,
                    'rt_values': rt_values,
                    'comment': comment,
                    'line_num': line_num,
                    'order': parsed_count
                }
                data_order.append(zaid)
            
            parsed_count += 1
            in_data_section = True
            print(f"[PARSE] Line {line_num}: ZAID={zaid}, Amin={amin}, Amax={amax}, N_values={len(rt_values)}")
            
            # Check if this is our target ZAID
            if zaid == target_zaid:
                params['amin'] = amin
                params['amax'] = amax
                params['rt_values'] = rt_values
                target_found = True
                print(f"[PARSE] Line {line_num}: *** TARGET ZAID FOUND ***")
                print(f"[PARSE]   R_T(A) range: A={amin} to A={amax}")
                print(f"[PARSE]   First 5 values: {rt_values[:5]}")
                print(f"[PARSE]   Last 5 values: {rt_values[-5:]}")
        
        except (ValueError, IndexError) as e:
            # Failed to parse as data, treat as footer
            if in_data_section:
                footer_lines.append(line)
                print(f"[PARSE] Line {line_num}: Could not parse as data (footer?): {e}")
            else:
                header_lines.append(line)
                print(f"[PARSE] Line {line_num}: Could not parse as data (header?): {e}")
    
    print(f"[PARSE] Summary: Read {line_count} lines, parsed {parsed_count} isotope entries")
    print(f"[PARSE] Header lines: {len(header_lines)}")
    print(f"[PARSE] Data entries: {len(data_lines)}")
    print(f"[PARSE] Footer lines: {len(footer_lines)}")
    
    if not target_found:
        available = sorted(data_lines.keys())
        raise ValueError(f"Target ZAID {target_zaid} not found in file. Available ZAIDs: {available}")
    
    print(f"[PARSE] Target parameters: Amin={params['amin']}, Amax={params['amax']}, N_values={len(params['rt_values'])}")
    
    if preserve_format:
        format_info = {
            'header_lines': header_lines,
            'data_lines': data_lines,
            'data_order': data_order,
            'footer_lines': footer_lines,
            'target_zaid': target_zaid,
            '_metadata': {
                'has_trailing_newline': has_trailing_newline,
                'all_zaids': sorted(data_lines.keys())
            }
        }
        return params, format_info
    else:
        return params, None

def write(filepath: Path, params: Dict[str, Any],
              format_info: Optional[Dict[str, Any]] = None,
              target_zaid: int = 92235,
              scale_factor: float = 1.0) -> None:
    """
    Write parameters to rta.dat file.
    
    CRITICAL FORMATTING REQUIREMENTS (matches CGMF C++ implementation):
    - Header MUST be exactly 18 characters total:
      * ZAID: positions [0:7] (7 chars, right-aligned)
      * Amin: positions [7:12] (5 chars, right-aligned)
      * Amax: positions [12:18] (6 chars, right-aligned)
    - Data array: starts at position 18, whitespace-delimited
    
    If header is not exactly 18 chars, CGMF's substr(18) will read wrong data!
    
    Format preservation:
    - If format_info provided: preserves entire file structure, modifying only target ZAID line
    - If format_info is None: raises error (cannot reconstruct file without format info)
    
    NOTE: All R_T(A) values for a single isotope are scaled by the same factor.
          scale_factor is applied to ALL elements in the rt_values array.
    
    Args:
        filepath: Path to write rta.dat
        params: Dictionary with 'amin', 'amax', 'rt_values'
        format_info: Format information from parse_rta (REQUIRED)
        target_zaid: ZAID being modified
        scale_factor: Multiplicative factor to apply to ALL R_T(A) values (default 1.0)
    """
    print(f"[WRITE] Writing to file: {filepath}")
    print(f"[WRITE] Target ZAID: {target_zaid}")
    print(f"[WRITE] Scale factor: {scale_factor}")
    print(f"[WRITE] Amin={params['amin']}, Amax={params['amax']}, N_values={len(params['rt_values'])}")
    print(f"[WRITE] Using HYBRID formatting: fixed-width 18-char header + whitespace-delimited data")
    
    if format_info is None:
        raise ValueError("format_info is required for writing rta.dat - cannot reconstruct file structure without it")
    
    # Validate required parameters
    if 'amin' not in params or 'amax' not in params or 'rt_values' not in params:
        raise ValueError("params must contain 'amin', 'amax', and 'rt_values'")
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Extract format info
    header_lines = format_info['header_lines']
    data_lines = format_info['data_lines']
    data_order = format_info['data_order']
    footer_lines = format_info['footer_lines']
    has_trailing_newline = format_info['_metadata']['has_trailing_newline']
    
    print(f"[WRITE] Reconstructing file with {len(header_lines)} header lines, {len(data_lines)} data entries, {len(footer_lines)} footer lines")
    
    if target_zaid not in data_lines:
        raise ValueError(f"Cannot write: target ZAID {target_zaid} not found in original file. Available: {sorted(data_lines.keys())}")
    
    lines = []
    
    # Write header
    lines.extend(header_lines)
    
    # Write data section IN ORIGINAL ORDER
    modified_count = 0
    for zaid in data_order:
        entry = data_lines[zaid]
        
        if zaid == target_zaid:
            # Apply scale factor to ALL values in the array
            scaled_values = [v * scale_factor for v in params['rt_values']]
            
            # Check if values changed (accounting for floating point precision)
            original_values = entry['rt_values']
            values_unchanged = all(abs(scaled_values[i] - original_values[i]) < 1e-15
                                  for i in range(len(scaled_values)))
            
            if values_unchanged and abs(scale_factor - 1.0) < 1e-15:
                # Unchanged - use original line
                line = entry['original_line']
                print(f"[WRITE] ZAID {zaid}: Unchanged (preserving original)")
            else:
                # Modified - reconstruct line with EXACT formatting
                
                # CRITICAL: Construct fixed-width 18-character header
                header = (
                    RTA_FORMATS['zaid'].format(int(zaid)) +
                    RTA_FORMATS['amin'].format(int(params['amin'])) +
                    RTA_FORMATS['amax'].format(int(params['amax']))
                )
                
                # Assertion to catch formatting errors during development
                assert len(header) == 17, f"RTA header must be exactly 18 chars, got {len(header)}: '{header}'"
                
                # Format data array (whitespace-delimited, starting at position 18)
                # Use full precision to avoid rounding issues
                data_str = ' '.join(f'{v:.6f}' for v in scaled_values)
                
                # Combine header + data
                line = f"{header} {data_str}"
                
                # Add comment if original had one
                if entry['comment']:
                    line += f" {entry['comment']}"
                
                modified_count += 1
                print(f"[WRITE] ZAID {zaid}: Modified with scale_factor={scale_factor}")
                print(f"[WRITE]   Header (18 chars): '{header}'")
                print(f"[WRITE]   First 5 scaled values: {scaled_values[:5]}")
                print(f"[WRITE]   Last 5 scaled values: {scaled_values[-5:]}")
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
