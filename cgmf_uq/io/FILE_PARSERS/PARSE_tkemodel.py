from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional


def parse(
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

def write(
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
