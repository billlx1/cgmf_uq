from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import re

def parse(filepath: Path, target_zaid: int = 92235, preserve_format: bool = True) -> Tuple[Dict[str, float], Optional[Dict[str, str]]]:
    """
    Parse gstrength_gdr_params.dat file.
    
    Format: key = value;
    - Whitespace around '=' and before ';' is flexible
    - Comments and blank lines may be present
    
    Args:
        filepath: Path to gstrength_gdr_params.dat
        preserve_format: If True, also return format information for exact reconstruction
        
    Returns:
        If preserve_format is False: Dictionary mapping parameter names to values
        If preserve_format is True: Tuple of (params dict, format dict with original line strings)
    """
    print(f"[PARSE] Reading file: {filepath}")
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    params = {}
    format_info = {} if preserve_format else None
    line_count = 0
    parsed_count = 0
    
    # Pattern: param_name = value;
    # Allows flexible whitespace
    pattern = re.compile(r'^(\s*)(\w+)(\s*=\s*)([^;]+)(;)(.*)$')
    
    with open(filepath, 'r') as f:
        content = f.read()
        has_trailing_newline = content.endswith('\n')
        lines = content.splitlines()
    
    for line_num, line in enumerate(lines, 1):
        line_count += 1
        
        # Skip empty lines
        if not line.strip():
            print(f"[PARSE] Line {line_num}: Empty line (skipped)")
            continue
        
        # Skip comment lines (if any start with # or //)
        if line.strip().startswith('#') or line.strip().startswith('//'):
            print(f"[PARSE] Line {line_num}: Comment (skipped)")
            continue
        
        # Try to match parameter line
        match = pattern.match(line)
        if match:
            leading_space = match.group(1)
            param_name = match.group(2)
            equals_spacing = match.group(3)
            param_value_str = match.group(4).strip()
            semicolon = match.group(5)
            trailing = match.group(6)
            
            # Convert to float
            try:
                param_value = float(param_value_str)
                params[param_name] = param_value
                parsed_count += 1
                
                # Store format information to preserve exact spacing
                if preserve_format:
                    format_info[param_name] = {
                        'original_line': line,
                        'leading_space': leading_space,
                        'equals_spacing': equals_spacing,
                        'value_str': param_value_str,  # Preserve original string representation
                        'trailing': trailing
                    }
                
                print(f"[PARSE] Line {line_num}: {param_name} = {param_value} (orig: '{param_value_str}')")
            except ValueError:
                print(f"[PARSE] WARNING: Line {line_num}: Could not convert '{param_value_str}' to float")
                raise ValueError(f"Invalid float value on line {line_num}: {param_value_str}")
        else:
            print(f"[PARSE] WARNING: Line {line_num}: Could not parse: {line.strip()}")
            raise ValueError(f"Invalid format on line {line_num}: {line.strip()}")
    
    print(f"[PARSE] Summary: Read {line_count} lines, parsed {parsed_count} parameters")
    print(f"[PARSE] File has trailing newline: {has_trailing_newline}")
    print(f"[PARSE] Parameters found: {list(params.keys())}")
    
    if preserve_format:
        format_info['_metadata'] = {'has_trailing_newline': has_trailing_newline}
        return params, format_info
    else:
        return params, None

def write(filepath: Path, params: Dict[str, float],
                       format_info: Optional[Dict[str, Any]] = None,
                       target_zaid: int = 92235,
                       scale_factors: Optional[Dict[str, float]] = None) -> None:
    """
    Write parameters to gstrength_gdr_params.dat file.
    
    Format preservation:
    - If format_info provided: preserves exact spacing and number formatting from original
    - If format_info is None: uses consistent formatting
    
    Args:
        filepath: Path to write gstrength_gdr_params.dat
        params: Dictionary of parameter names to values
        format_info: Optional format information from parse_gstrength_gdr
        scale_factors: Optional dict mapping parameter names to scaling factors.
                      If provided, each parameter is scaled: new_value = original_value * scale_factor
                      Parameters not in scale_factors use scale factor of 1.0 (unchanged)
    """
    print(f"[WRITE] Writing to file: {filepath}")
    print(f"[WRITE] Writing {len(params)} parameters")
    print(f"[WRITE] Format preservation: {'ON' if format_info else 'OFF'}")
    print(f"[WRITE] Scale factors: {'ON' if scale_factors else 'OFF'}")
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Apply scale factors if provided
    scaled_params = {}
    for param_name, value in params.items():
        if scale_factors and param_name in scale_factors:
            scaled_value = value * scale_factors[param_name]
            scaled_params[param_name] = scaled_value
            print(f"[WRITE] Scaling {param_name}: {value} * {scale_factors[param_name]} = {scaled_value}")
        else:
            scaled_params[param_name] = value
    
    # Define the canonical order (based on the example file)
    canonical_order = [
        'global_PSF_norm',
        'E1_DArigo_E_const1',
        'E1_DArigo_E_const2',
        'E1_DArigo_E_exp',
        'E1_DArigo_W_factor',
        'E1_DArigo_S_coef',
        'E1_DH0_E_const',
        'E1_DH0_E_exp_mass',
        'E1_DH0_E_exp_beta',
        'E1_DH0_W_const',
        'E1_DH0_W_beta_coef',
        'E1_DH0_S_coef',
        'E1_DH1_E_const',
        'E1_DH1_E_exp_mass',
        'E1_DH1_W_const',
        'E1_DH1_W_beta_coef',
        'E1_DH1_S_coef',
        'M1_E_const',
        'M1_E_exp',
        'M1_W_val',
        'M1_S_val',
        'E2_E_const',
        'E2_E_exp',
        'E2_W_const',
        'E2_W_mass_coef',
        'E2_S_coef'
    ]
    
    # Check for any parameters not in canonical order
    extra_params = set(scaled_params.keys()) - set(canonical_order)
    if extra_params:
        print(f"[WRITE] WARNING: Extra parameters not in canonical order: {extra_params}")
    
    missing_params = set(canonical_order) - set(scaled_params.keys())
    if missing_params:
        print(f"[WRITE] WARNING: Missing expected parameters: {missing_params}")
    
    # Determine if we should add trailing newline
    has_trailing_newline = True
    if format_info and '_metadata' in format_info:
        has_trailing_newline = format_info['_metadata'].get('has_trailing_newline', True)
    
    lines = []
    
    # Write parameters in canonical order
    for param_name in canonical_order:
        if param_name in scaled_params:
            value = scaled_params[param_name]
            
            if format_info and param_name in format_info:
                # Preserve exact format from original
                fmt = format_info[param_name]
                
                # Check if value changed
                original_value = params[param_name]  # Use unscaled original for comparison
                if abs(value - original_value) < 1e-15:  # Essentially unchanged (float precision)
                    # Use original string representation
                    line = fmt['original_line']
                    print(f"[WRITE] Wrote (preserved): {param_name} = {fmt['value_str']}")
                else:
                    # Value changed, need to format new value
                    # Try to match the style of the original
                    if 'e' in fmt['value_str'].lower():
                        # Use scientific notation
                        value_str = f"{value:g}"
                    else:
                        # Use decimal notation
                        value_str = str(value)
                    
                    line = f"{fmt['leading_space']}{param_name}{fmt['equals_spacing']}{value_str};{fmt['trailing']}"
                    print(f"[WRITE] Wrote (modified): {param_name} = {value_str}")
            else:
                # No format info, use default formatting
                line = f"{param_name:<23} = {value};"
                print(f"[WRITE] Wrote (default): {param_name} = {value}")
            
            lines.append(line)
    
    # Write any extra parameters at the end
    for param_name in sorted(extra_params):
        value = scaled_params[param_name]
        line = f"{param_name:<23} = {value};"
        lines.append(line)
        print(f"[WRITE] Wrote (extra): {param_name} = {value}")
    
    # Write to file
    with open(filepath, 'w') as f:
        for i, line in enumerate(lines):
            f.write(line)
            # Add newline between lines, but maybe not after last line
            if i < len(lines) - 1:
                f.write('\n')
            elif has_trailing_newline:
                f.write('\n')
    
    print(f"[WRITE] Successfully wrote {len(scaled_params)} parameters to {filepath}")
    print(f"[WRITE] Trailing newline: {has_trailing_newline}")
