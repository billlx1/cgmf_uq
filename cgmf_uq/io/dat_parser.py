"""
Parser for CGMF .dat files.

This module provides parsing and writing capabilities for the various .dat file
formats used by CGMF. Each file type has its own structure that must be preserved.
"""

from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import re


# Format specifications matching CGMF C++ parser requirements
RTA_FORMATS = {
    'zaid': '{:^7}',     # Width 7, right-aligned (handles negative SF)
    'amin': '{:^5}',     # Width 5, right-aligned
    'amax': '{:^5}',     # Width 6, right-aligned
    'data': '{:>6.3f}'   # Width 6, 3 decimals (data array elements)
}

class DatParser:
    """Handles parsing and writing of CGMF .dat files."""
    
    """
Refactored parse_gstrength_gdr and write_gstrength_gdr methods for DatParser class.
Replace the existing methods in your DatParser class with these.
"""

    @staticmethod
    def parse_gstrength_gdr(filepath: Path, preserve_format: bool = True) -> Tuple[Dict[str, float], Optional[Dict[str, str]]]:
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

    @staticmethod
    def write_gstrength_gdr(filepath: Path, params: Dict[str, float],
                           format_info: Optional[Dict[str, Any]] = None,
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


    @staticmethod
    def parse_spinscaling(filepath: Path, target_zaid: int = 92235, preserve_format: bool = True) -> Tuple[Dict[str, float], Optional[Dict[str, Any]]]:
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


    @staticmethod
    def write_spinscaling(filepath: Path, params: Dict[str, float],
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
        
    
    """
    Fixed parse_rta and write_rta methods for DatParser class.

    These methods correctly implement the HYBRID parsing strategy used by CGMF:
    - Fixed-width header (ZAID, Amin, Amax) at exact character positions [0:7], [7:12], [12:18]
    - Whitespace-delimited data array starting at index 18

    CRITICAL: The header MUST be exactly 18 characters or CGMF will read garbage data.
    """

    @staticmethod
    def parse_rta(filepath: Path, target_zaid: int = 92235, preserve_format: bool = True) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
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


    @staticmethod
    def write_rta(filepath: Path, params: Dict[str, Any],
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


    """
    Refactored tkemodel parsing and writing functions.
    Replace the existing methods in your DatParser class with these.
    """

    @staticmethod
    def parse_tkemodel(
        filepath: Path,
        target_zaid: int = 92235,
        preserve_format: bool = True
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Parse tkemodel.dat using CGMF-equivalent logic.
        
        File format: 27 whitespace-delimited tokens per line
        Token 1: ZAID (compound nucleus)
        Tokens 2-27: 26 float parameters organized as:
          - Tokens 2-5: tke_en (4 values) - TKE vs neutron energy params
          - Tokens 6-16: tke_ah (11 values) - TKE vs heavy fragment mass params
          - Tokens 17-27: sigma_tke (11 values) - TKE variance params
        
        NOTE: ZAID interpretation in this file:
              - ZAIDs represent COMPOUND nucleus (target + neutron)
              - For neutron-induced: compound_zaid = target_zaid + 1
              - Negative ZAIDs: Spontaneous fission
        
        Args:
            filepath: Path to tkemodel.dat
            target_zaid: ZAID of target nucleus (e.g., 92235 for U-235(n,f))
            preserve_format: If True, return format information for reconstruction
            
        Returns:
            If preserve_format is False: Dictionary with 'tke_en', 'tke_ah', 'sigma_tke'
            If preserve_format is True: Tuple of (params dict, format dict)
        """
        print(f"[PARSE] Reading file: {filepath}")
        print(f"[PARSE] Target ZAID: {target_zaid}")
        
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        # Convert target ZAID to compound ZAID
        if target_zaid > 0:
            compound_zaid = target_zaid + 1
            print(f"[PARSE] Compound ZAID (for file lookup): {compound_zaid} (target + neutron)")
        else:
            compound_zaid = target_zaid
            print(f"[PARSE] Spontaneous fission ZAID: {compound_zaid}")
        
        params = {}
        format_info = {} if preserve_format else None
        
        preamble_lines: list[str] = []
        data_records: list[dict] = []
        
        found_target = False
        seen_data = False
        
        with open(filepath, 'r') as f:
            content = f.read()
            has_trailing_newline = content.endswith('\n')
            lines = content.splitlines()
        
        print(f"[PARSE] File has {len(lines)} lines")
        print(f"[PARSE] File has trailing newline: {has_trailing_newline}")
        
        for line_num, raw in enumerate(lines, start=1):
            stripped = raw.strip()
            
            # Blank or comment lines
            if not stripped or stripped.startswith('#'):
                if not seen_data:
                    preamble_lines.append(raw)
                continue
            
            # Try to parse as data line (EXACT CGMF behavior: 27 tokens)
            tokens = stripped.split()
            
            if len(tokens) != 27:
                if not seen_data:
                    preamble_lines.append(raw)
                continue
            
            try:
                zaid = int(tokens[0])
                values = [float(tok) for tok in tokens[1:]]
            except ValueError:
                if not seen_data:
                    preamble_lines.append(raw)
                continue
            
            seen_data = True
            
            # Store record for format preservation
            record = {
                'zaid': zaid,
                'values': values,  # All 26 floats
                'original_line': raw,
                'line_num': line_num,
            }
            data_records.append(record)
            
            print(f"[PARSE] Line {line_num}: ZAID={zaid}, N_params={len(values)}")
            
            # Check if this is our target
            if zaid == compound_zaid:
                # Split into logical groups matching CGMF structure
                params['tke_en'] = values[0:4]      # Tokens 2-5
                params['tke_ah'] = values[4:15]     # Tokens 6-16
                params['sigma_tke'] = values[15:26] # Tokens 17-27
                found_target = True
                print(f"[PARSE] Line {line_num}: *** COMPOUND ZAID FOUND (target={target_zaid}) ***")
                print(f"[PARSE]   tke_en: {params['tke_en']}")
                print(f"[PARSE]   tke_ah: {params['tke_ah']}")
                print(f"[PARSE]   sigma_tke: {params['sigma_tke']}")
        
        print(f"[PARSE] Summary: Parsed {len(data_records)} isotope entries")
        print(f"[PARSE] Preamble lines: {len(preamble_lines)}")
        
        if not found_target:
            available = [rec['zaid'] for rec in data_records]
            raise ValueError(
                f"Target ZAID {target_zaid} (compound ZAID {compound_zaid}) not found in file. "
                f"Available ZAIDs: {sorted(available)}"
            )
        
        if preserve_format:
            format_info = {
                'preamble_lines': preamble_lines,
                'data_records': data_records,
                'target_zaid': target_zaid,
                'compound_zaid': compound_zaid,
                '_metadata': {
                    'has_trailing_newline': has_trailing_newline,
                    'all_zaids': sorted([rec['zaid'] for rec in data_records])
                }
            }
            return params, format_info
        else:
            return params, None


    @staticmethod
    def write_tkemodel(
        filepath: Path,
        params: Dict[str, list[float]],
        format_info: Optional[Dict[str, Any]] = None,
        target_zaid: int = 92235,
        tke_en_scales: Optional[list[float]] = None,
        tke_ah_scales: Optional[list[float]] = None,
        sigma_tke_scales: Optional[list[float]] = None
    ) -> None:
        """
        Write parameters to tkemodel.dat file.
        
        Format preservation:
        - If format_info provided: preserves entire file structure, modifying only target ZAID line
        - If format_info is None: raises error (cannot reconstruct file without format info)
        
        Scaling: Each parameter can be scaled independently
        - tke_en_scales: list of 4 scale factors for tke_en parameters (default: [1.0, 1.0, 1.0, 1.0])
        - tke_ah_scales: list of 11 scale factors for tke_ah parameters (default: all 1.0)
        - sigma_tke_scales: list of 11 scale factors for sigma_tke parameters (default: all 1.0)
        
        Args:
            filepath: Path to write tkemodel.dat
            params: Dictionary with 'tke_en', 'tke_ah', 'sigma_tke' lists
            format_info: Format information from parse_tkemodel (REQUIRED)
            target_zaid: ZAID of target nucleus being modified
            tke_en_scales: Scale factors for each tke_en parameter
            tke_ah_scales: Scale factors for each tke_ah parameter
            sigma_tke_scales: Scale factors for each sigma_tke parameter
        """
        print(f"[WRITE] Writing to file: {filepath}")
        print(f"[WRITE] Target ZAID: {target_zaid}")
        
        if format_info is None:
            raise ValueError("format_info is required for writing tkemodel.dat - cannot reconstruct file structure without it")
        
        # Validate parameter structure
        if 'tke_en' not in params or 'tke_ah' not in params or 'sigma_tke' not in params:
            raise ValueError("params must contain 'tke_en', 'tke_ah', and 'sigma_tke'")
        
        if len(params['tke_en']) != 4:
            raise ValueError(f"tke_en must have 4 values, got {len(params['tke_en'])}")
        if len(params['tke_ah']) != 11:
            raise ValueError(f"tke_ah must have 11 values, got {len(params['tke_ah'])}")
        if len(params['sigma_tke']) != 11:
            raise ValueError(f"sigma_tke must have 11 values, got {len(params['sigma_tke'])}")
        
        # Ensure parent directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Set default scale factors (no scaling)
        if tke_en_scales is None:
            tke_en_scales = [1.0] * 4
        if tke_ah_scales is None:
            tke_ah_scales = [1.0] * 11
        if sigma_tke_scales is None:
            sigma_tke_scales = [1.0] * 11
        
        # Validate scale factor lengths
        if len(tke_en_scales) != 4:
            raise ValueError(f"tke_en_scales must have 4 values, got {len(tke_en_scales)}")
        if len(tke_ah_scales) != 11:
            raise ValueError(f"tke_ah_scales must have 11 values, got {len(tke_ah_scales)}")
        if len(sigma_tke_scales) != 11:
            raise ValueError(f"sigma_tke_scales must have 11 values, got {len(sigma_tke_scales)}")
        
        print(f"[WRITE] tke_en_scales: {tke_en_scales}")
        print(f"[WRITE] tke_ah_scales: {tke_ah_scales}")
        print(f"[WRITE] sigma_tke_scales: {sigma_tke_scales}")
        
        # Extract format info
        preamble_lines = format_info['preamble_lines']
        data_records = format_info['data_records']
        compound_zaid = format_info['compound_zaid']
        has_trailing_newline = format_info['_metadata']['has_trailing_newline']
        
        print(f"[WRITE] Compound ZAID (in file): {compound_zaid}")
        print(f"[WRITE] Reconstructing file with {len(preamble_lines)} preamble lines, {len(data_records)} data entries")
        
        # Apply per-parameter scaling
        scaled_tke_en = [v * s for v, s in zip(params['tke_en'], tke_en_scales)]
        scaled_tke_ah = [v * s for v, s in zip(params['tke_ah'], tke_ah_scales)]
        scaled_sigma_tke = [v * s for v, s in zip(params['sigma_tke'], sigma_tke_scales)]
        
        # Combine into single array (26 values)
        new_values = scaled_tke_en + scaled_tke_ah + scaled_sigma_tke
        assert len(new_values) == 26, f"Expected 26 values, got {len(new_values)}"
        
        # Fixed-width formatting to ensure 27 tokens (CRITICAL for CGMF parser)
        # Width 15 with 6 decimal places in scientific notation ensures whitespace separation
        # fmt_zaid = "{:>9d}"
        fmt_zaid = "{:d}"
        fmt_val = "{:>15.6E}"
        
        output_lines: list[str] = []
        
        # Write preamble verbatim
        output_lines.extend(preamble_lines)
        
        # Write data records
        modified_count = 0
        for rec in data_records:
            zaid = rec['zaid']
            
            if zaid != compound_zaid:
                # Preserve other isotopes unchanged
                output_lines.append(rec['original_line'])
                continue
            
            # Reconstruct target line with scaled values
            body = fmt_zaid.format(zaid) + "".join(fmt_val.format(v) for v in new_values)
            
            # CRITICAL SAFETY CHECK: Verify token count
            if len(body.split()) != 27:
                raise RuntimeError(
                    f"Generated tkemodel line has {len(body.split())} tokens (expected 27). "
                    f"Line: {body}"
                )
            
            output_lines.append(body)
            modified_count += 1
            
            print(f"[WRITE] ZAID {zaid} (target {target_zaid}): Modified")
            print(f"[WRITE]   Scaled tke_en: {scaled_tke_en}")
            print(f"[WRITE]   Scaled tke_ah (first 3): {scaled_tke_ah[:3]}")
            print(f"[WRITE]   Scaled sigma_tke (first 3): {scaled_sigma_tke[:3]}")
        
        # Write to file
        with open(filepath, 'w') as f:
            for i, line in enumerate(output_lines):
                f.write(line)
                if i < len(output_lines) - 1 or has_trailing_newline:
                    f.write('\n')
        
        print(f"[WRITE] Successfully wrote {len(data_records)} data entries to {filepath}")
        print(f"[WRITE] Modified {modified_count} entries")
        print(f"[WRITE] Trailing newline: {has_trailing_newline}")


    # ============================================================================
    # Placeholder methods for other .dat file types
    # ============================================================================
    
    @staticmethod
    def parse_other_dat_file(filepath: Path) -> Dict[str, Any]:
        """
        Parse another .dat file type.
        
        TODO: Implement based on file structure
        - Identify file format (tabular, key-value, mixed)
        - Handle comments
        - Parse data types (int, float, string, arrays)
        - Preserve structure information for writing
        """
        raise NotImplementedError("Parser for this file type not yet implemented")
    
    @staticmethod
    def write_other_dat_file(filepath: Path, data: Dict[str, Any]) -> None:
        """
        Write another .dat file type.
        
        TODO: Implement based on file structure
        - Maintain original formatting
        - Preserve comments (if needed)
        - Write in correct order
        """
        raise NotImplementedError("Writer for this file type not yet implemented")


# ============================================================================
# Helper functions for file identification
# ============================================================================

# ============================================================================
# Helper functions for file identification
# ============================================================================

def identify_dat_file_type(filepath: Path) -> str:
    """
    Identify the type of .dat file based on filename or content.
    """
    filename = filepath.name.lower()
    
    if 'gstrength_gdr' in filename:
        return 'gstrength_gdr'
    elif 'spinscaling' in filename:
        return 'spinscaling'
    elif 'rta' in filename:
        return 'rta'
    elif 'tkemodel' in filename:
        return 'tkemodel'
    else:
        return 'unknown'

def parse_dat_file(filepath: Path, preserve_format: bool = True, **kwargs) -> Tuple[Dict[str, Any], Optional[Dict]]:
    """Automatically parse a .dat file based on its type."""
    file_type = identify_dat_file_type(filepath)
    parser = DatParser()
    
    if file_type == 'gstrength_gdr':
        return parser.parse_gstrength_gdr(filepath, preserve_format)
    elif file_type == 'spinscaling':
        target_zaid = kwargs.get('target_zaid', 92235)
        return parser.parse_spinscaling(filepath, target_zaid, preserve_format)
    elif file_type == 'rta':
        target_zaid = kwargs.get('target_zaid', 92235)
        return parser.parse_rta(filepath, target_zaid, preserve_format)
    elif file_type == 'tkemodel':
        target_zaid = kwargs.get('target_zaid', 92235)
        return parser.parse_tkemodel(filepath, target_zaid, preserve_format)
    else:
        raise ValueError(f"Unknown .dat file type: {filepath.name}")

def write_dat_file(filepath: Path, data: Dict[str, Any], format_info: Optional[Dict] = None, **kwargs) -> None:
    """Automatically write a .dat file based on its type."""
    file_type = identify_dat_file_type(filepath)
    parser = DatParser()
    
    if file_type == 'gstrength_gdr':
        scale_factors = kwargs.get('scale_factors')
        parser.write_gstrength_gdr(filepath, data, format_info, scale_factors)
    
    elif file_type == 'spinscaling':
        if format_info and 'target_zaid' in format_info:
            target_zaid = format_info['target_zaid']
        else:
            target_zaid = kwargs.get('target_zaid', 92235)
        alpha_0_scale = kwargs.get('alpha_0_scale', 1.0)
        alpha_slope_scale = kwargs.get('alpha_slope_scale', 1.0)
        parser.write_spinscaling(filepath, data, format_info, target_zaid, alpha_0_scale, alpha_slope_scale)
    
    elif file_type == 'rta':
        if format_info and 'target_zaid' in format_info:
            target_zaid = format_info['target_zaid']
        else:
            target_zaid = kwargs.get('target_zaid', 92235)
        scale_factor = kwargs.get('scale_factor', 1.0)
        parser.write_rta(filepath, data, format_info, target_zaid, scale_factor)
    
    elif file_type == 'tkemodel':
        if format_info and 'target_zaid' in format_info:
            target_zaid = format_info['target_zaid']
        else:
            target_zaid = kwargs.get('target_zaid', 92235)
        tke_en_scales = kwargs.get('tke_en_scales')
        tke_ah_scales = kwargs.get('tke_ah_scales')
        sigma_tke_scales = kwargs.get('sigma_tke_scales')
        parser.write_tkemodel(filepath, data, format_info, target_zaid,
                            tke_en_scales, tke_ah_scales, sigma_tke_scales)
    
    else:
        raise ValueError(f"Unknown .dat file type: {filepath.name}")

