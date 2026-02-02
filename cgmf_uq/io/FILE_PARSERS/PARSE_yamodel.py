"""
CGMF yamodel.dat Parser and Writer

Handles the 3-Gaussian parameterization for pre-neutron emission mass yields Y(A).
Each line contains 15 parameters describing energy-dependent Gaussian components.

File Format:
    ZAID_compound w_a1 w_b1 mu_a1 mu_b1 sig_a1 sig_b1 w_a2 w_b2 mu_a2 mu_b2 sig_a2 sig_b2 sig_a0 sig_b0 # [refs]

CRITICAL ZAID INTERPRETATION:
    - For neutron-induced fission: ZAID is the COMPOUND nucleus (target + neutron)
      Example: For U-235(n,f), target_zaid=92235 → compound_zaid=92236 in file
    - For spontaneous fission: ZAID is NEGATIVE of the fissioning nucleus
      Example: For Cf-252(sf), target_zaid=-98252 → compound_zaid=-98252 in file
    
    The parser uses target_zaid (user-facing) but looks up compound_zaid (file storage).

Parameters:
    MY_AS1_Wa, MY_AS1_Wb: Weight parameters for 1st asymmetric peak
    MY_AS1_Mua, MY_AS1_Mub: Mean parameters for 1st asymmetric peak
    MY_AS1_Siga, MY_AS1_Sigb: Variance parameters for 1st asymmetric peak
    MY_AS2_Wa, MY_AS2_Wb: Weight parameters for 2nd asymmetric peak
    MY_AS2_Mua, MY_AS2_Mub: Mean parameters for 2nd asymmetric peak
    MY_AS2_Siga, MY_AS2_Sigb: Variance parameters for 2nd asymmetric peak
    MY_S_Siga, MY_S_Sigb: Variance parameters for symmetric peak
"""

from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

# ============================================================================
# FORMAT SPECIFICATIONS
# ============================================================================

YAMODEL_PARAM_NAMES = [
    'MY_AS1_Wa', 'MY_AS1_Wb', 'MY_AS1_Mua', 'MY_AS1_Mub', 
    'MY_AS1_Siga', 'MY_AS1_Sigb',
    'MY_AS2_Wa', 'MY_AS2_Wb', 'MY_AS2_Mua', 'MY_AS2_Mub',
    'MY_AS2_Siga', 'MY_AS2_Sigb',
    'MY_S_Siga', 'MY_S_Sigb'
]


# ============================================================================
# YAMODEL PARSER
# ============================================================================

def parse(
    filepath: Path,
    target_zaid: int = 92235,
    preserve_format: bool = True
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    Parse yamodel.dat file for 3-Gaussian Y(A) parameterization.
    
    Reads the 14-parameter energy-dependent Gaussian fit for mass yields.
    Supports multi-chance fission by storing data for compound nucleus and
    its lighter daughters (compound-1, compound-2, compound-3).
    
    CRITICAL: The file stores COMPOUND nucleus ZAIDs for neutron-induced fission.
              For target U-235, we look for compound ZAID 92236.
              For spontaneous fission, ZAID is negative and unchanged.
    
    NOTE: ZAID Interpretation
        - target_zaid > 0: Neutron-induced fission
          File lookup: compound_zaid = target_zaid + 1
          Example: target_zaid=92235 → searches for 92236 in file
        
        - target_zaid < 0: Spontaneous fission  
          File lookup: compound_zaid = target_zaid (unchanged)
          Example: target_zaid=-98252 → searches for -98252 in file
    
    Args:
        filepath: Path to yamodel.dat file
        target_zaid: Target nucleus ZAID (user-facing identifier)
        preserve_format: If True, return format preservation metadata
    
    Returns:
        If preserve_format is False:
            Tuple of (params_dict, None)
        If preserve_format is True:
            Tuple of (params_dict, format_info) where format_info contains:
            - header_lines: Lines before data section
            - data_lines: Dict mapping ZAID → line metadata
            - data_order: Original order of ZAIDs (NOT sorted)
            - footer_lines: Lines after data section
            - target_zaid: User-provided target ZAID
            - compound_zaid: Actual ZAID looked up in file
            - _metadata: has_trailing_newline, all_zaids
    """
    print(f"[PARSE] Reading file: {filepath}")
    print(f"[PARSE] Target ZAID: {target_zaid}")
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    # Determine compound nucleus ZAID for file lookup
    if target_zaid > 0:
        compound_zaid = target_zaid + 1
        print(f"[PARSE] Neutron-induced fission mode")
        print(f"[PARSE] Compound ZAID: {compound_zaid} (target + neutron)")
    else:
        compound_zaid = target_zaid
        print(f"[PARSE] Spontaneous fission mode")
        print(f"[PARSE] ZAID: {compound_zaid}")
    
    with open(filepath, 'r') as f:
        content = f.read()
        has_trailing_newline = content.endswith('\n')
        lines = content.splitlines()
    
    print(f"[PARSE] File has {len(lines)} lines")
    print(f"[PARSE] File has trailing newline: {has_trailing_newline}")
    
    header_lines = []
    data_lines = {}
    data_order = []
    footer_lines = []
    
    line_count = 0
    parsed_count = 0
    in_data_section = False
    target_found = False
    target_params = {}
    
    for line_num, line in enumerate(lines, 1):
        line_count += 1
        
        # Empty lines
        if not line.strip():
            if not in_data_section:
                header_lines.append(line)
            else:
                footer_lines.append(line)
            continue
        
        # Comment lines
        if line.strip().startswith('#'):
            if not in_data_section:
                header_lines.append(line)
            else:
                footer_lines.append(line)
            continue
        
        # Try to parse as data line
        try:
            # Split line to extract ZAID and parameters
            # Format: ZAID param1 param2 ... param14 # [optional comment]
            
            # Check for inline comment
            if '#' in line:
                data_part, comment_part = line.split('#', 1)
                comment = '#' + comment_part
            else:
                data_part = line
                comment = ''
            
            tokens = data_part.split()
            
            if len(tokens) < 15:
                # Not enough tokens - treat as footer
                footer_lines.append(line)
                continue
            
            # Parse ZAID (first token)
            try:
                zaid = int(tokens[0])
            except ValueError:
                # Not a valid ZAID - treat as footer
                footer_lines.append(line)
                continue
            
            # Parse 14 parameters
            try:
                params = [float(tokens[i]) for i in range(1, 15)]
            except ValueError as e:
                print(f"[PARSE] WARNING: Line {line_num}: Could not convert parameter to float")
                raise ValueError(f"Invalid parameter value on line {line_num}: {e}")
            
            # We've successfully parsed a data line
            in_data_section = True
            parsed_count += 1
            
            # Store in data_lines
            param_dict = {
                YAMODEL_PARAM_NAMES[i]: params[i]
                for i in range(14)
            }
            
            data_lines[zaid] = {
                'original_line': line,
                'comment': comment,
                'line_num': line_num,
                'order': parsed_count,
                **param_dict
            }
            
            data_order.append(zaid)
            
            # Check if this is our target (or any of the daughter nuclei for multi-chance)
            is_target = (zaid == compound_zaid)
            is_daughter1 = (zaid == compound_zaid - 1)
            is_daughter2 = (zaid == compound_zaid - 2)
            is_daughter3 = (zaid == compound_zaid - 3)
            
            if is_target:
                print(f"[PARSE] Line {line_num}: *** TARGET ZAID FOUND ***")
                print(f"[PARSE] Line {line_num}: ZAID={zaid}")
                target_found = True
                target_params = param_dict.copy()
            elif is_daughter1:
                print(f"[PARSE] Line {line_num}: 2nd-chance daughter found: {zaid}")
            elif is_daughter2:
                print(f"[PARSE] Line {line_num}: 3rd-chance daughter found: {zaid}")
            elif is_daughter3:
                print(f"[PARSE] Line {line_num}: 4th-chance daughter found: {zaid}")
            else:
                print(f"[PARSE] Line {line_num}: ZAID={zaid} (not target)")
            
        except Exception as e:
            # Failed to parse - treat as footer
            footer_lines.append(line)
            print(f"[PARSE] Line {line_num}: Treated as footer (parse failed)")
            continue
    
    print(f"[PARSE] Summary: Read {line_count} lines, parsed {parsed_count} data lines")
    print(f"[PARSE] Header lines: {len(header_lines)}")
    print(f"[PARSE] Data entries: {len(data_lines)}")
    print(f"[PARSE] Footer lines: {len(footer_lines)}")
    
    if not target_found:
        available = sorted(data_lines.keys())
        raise ValueError(
            f"Target compound ZAID {compound_zaid} not found in file. "
            f"Available ZAIDs: {available}"
        )
    
    # FIX 1: Add target parameters print (matching standard pattern)
    print(f"[PARSE] Target parameters:")
    for param_name in YAMODEL_PARAM_NAMES:
        print(f"[PARSE]   {param_name} = {target_params[param_name]:.6f}")
    
    if preserve_format:
        format_info = {
            'header_lines': header_lines,
            'data_lines': data_lines,
            'data_order': data_order,
            'footer_lines': footer_lines,
            'target_zaid': target_zaid,
            'compound_zaid': compound_zaid,
            '_metadata': {
                'has_trailing_newline': has_trailing_newline,
                'all_zaids': data_order.copy()
            }
        }
        return target_params, format_info
    else:
        return target_params, None


def write(
    filepath: Path,
    params: Dict[str, Any],
    format_info: Optional[Dict[str, Any]] = None,
    target_zaid: int = 92235,
    scale_factors: Optional[Dict[str, float]] = None
) -> None:
    """
    Write yamodel.dat file with updated parameters.
    
    Format preservation:
    - If format_info provided: preserves entire file structure, modifying only target ZAID line
    - If format_info is None: raises error (cannot reconstruct file without format info)
    
    CRITICAL: Only the target ZAID line is modified. All other lines
              (daughter nuclei, other isotopes) are preserved exactly.
    
    NOTE: ZAID Handling
        - Uses compound_zaid from format_info for file operations
        - For neutron-induced: compound_zaid = target_zaid + 1
        - For spontaneous fission: compound_zaid = target_zaid (unchanged)
    
    NOTE: Each parameter (MY_AS1_Wa, MY_AS1_Mua, etc.) is scaled independently.
    
    Args:
        filepath: Output file path
        params: Dictionary of base parameter values (keys from YAMODEL_PARAM_NAMES)
        format_info: Format preservation metadata from parse() (REQUIRED)
        target_zaid: Target nucleus ZAID
        scale_factors: Dict mapping parameter names to scaling factors
                      Example: {'MY_AS1_Wa': 1.1, 'MY_AS1_Mub': 0.95}
                      Omitted parameters default to scale factor of 1.0
    
    Raises:
        ValueError: If format_info is None (cannot reconstruct file structure)
        ValueError: If required parameters missing from params dict
        ValueError: If target ZAID not found in format_info
    
    Returns:
        None (writes file to disk)
    """
    print(f"[WRITE] Writing to file: {filepath}")
    print(f"[WRITE] Target ZAID: {target_zaid}")
    
    if format_info is None:
        raise ValueError("format_info is required for writing yamodel.dat - cannot reconstruct file structure without it")
    
    # Validate required parameters
    if not all(param in params for param in YAMODEL_PARAM_NAMES):
        missing = [p for p in YAMODEL_PARAM_NAMES if p not in params]
        raise ValueError(f"params missing required parameters: {missing}")
    
    # Ensure output directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Apply scaling factors
    if scale_factors is None:
        scale_factors = {}
    
    scaled_params = {}
    for param_name in YAMODEL_PARAM_NAMES:
        base_value = params[param_name]
        scale = scale_factors.get(param_name, 1.0)
        scaled_params[param_name] = base_value * scale
        
        if abs(scale - 1.0) > 1e-15:
            print(f"[WRITE] Scaling {param_name}: {base_value:.6f} → {scaled_params[param_name]:.6f} (×{scale:.4f})")
    
    # Extract format info components
    header_lines = format_info['header_lines']
    data_lines = format_info['data_lines']
    data_order = format_info['data_order']
    footer_lines = format_info['footer_lines']
    has_trailing_newline = format_info['_metadata']['has_trailing_newline']
    
    # FIX 2: Add compound_zaid fallback calculation (backward compatibility)
    compound_zaid = format_info.get('compound_zaid')
    
    if compound_zaid is None:
        if target_zaid > 0:
            compound_zaid = target_zaid + 1
            print(f"[WRITE] Calculated compound ZAID: {compound_zaid} (from target {target_zaid})")
        else:
            compound_zaid = target_zaid
            print(f"[WRITE] Spontaneous fission ZAID: {compound_zaid}")
    else:
        print(f"[WRITE] Using stored compound ZAID: {compound_zaid}")
    
    print(f"[WRITE] Reconstructing file with {len(header_lines)} header lines, {len(data_lines)} data entries, {len(footer_lines)} footer lines")
    # FIX 3: Add order preservation statement (matching standard)
    print(f"[WRITE] Using ORIGINAL ORDER (not sorted): {data_order}")
    
    if compound_zaid not in data_lines:
        raise ValueError(f"Cannot write: target compound ZAID {compound_zaid} not found in original file. Available: {sorted(data_lines.keys())}")
    
    # Build output lines
    output_lines = []
    
    # Write header
    output_lines.extend(header_lines)
    
    # Write data lines in original order
    modified_count = 0
    for zaid in data_order:
        line_info = data_lines[zaid]
        
        if zaid == compound_zaid:
            # Reconstruct the target line with scaled parameters
            print(f"[WRITE] Reconstructing line for target ZAID: {zaid}")
            
            # Check if parameters actually changed
            original_params = {
                param_name: line_info[param_name]
                for param_name in YAMODEL_PARAM_NAMES
            }
            
            params_changed = False
            for param_name in YAMODEL_PARAM_NAMES:
                if abs(scaled_params[param_name] - original_params[param_name]) > 1e-15:
                    params_changed = True
                    break
            
            if not params_changed:
                # No actual changes - use original line
                print(f"[WRITE] No parameter changes detected - preserving original formatting")
                output_lines.append(line_info['original_line'])
            else:
                # Reconstruct line with new values
                param_values = [scaled_params[name] for name in YAMODEL_PARAM_NAMES]
                
                # Format: ZAID followed by 14 parameters
                # Use sufficient precision to avoid physics errors
                new_line = f"{zaid:6d}"
                for val in param_values:
                    new_line += f" {val:12.6f}"
                
                # Append comment if present
                if line_info['comment']:
                    new_line += f" {line_info['comment']}"
                
                output_lines.append(new_line)
                print(f"[WRITE] Reconstructed line: {new_line[:80]}...")
                modified_count += 1
        else:
            # Not the target - preserve original line exactly
            output_lines.append(line_info['original_line'])
    
    # Write footer
    output_lines.extend(footer_lines)
    
    # Write to file
    with open(filepath, 'w') as f:
        for i, line in enumerate(output_lines):
            f.write(line)
            if i < len(output_lines) - 1:
                f.write('\n')
            elif has_trailing_newline:
                f.write('\n')
    
    print(f"[WRITE] Successfully wrote {len(data_lines)} data entries to {filepath}")
    print(f"[WRITE] Modified {modified_count} entries")
    print(f"[WRITE] Trailing newline: {has_trailing_newline}")
