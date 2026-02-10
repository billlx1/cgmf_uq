"""
Parser for CGMF .dat files.
This module provides parsing and writing capabilities for the various .dat file
formats used by CGMF. Each file type has its own structure that must be preserved.
"""
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import sys

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from cgmf_uq.io.FILE_PARSERS import (
    PARSE_gstrength, 
    PARSE_spinscaling, 
    PARSE_rta, 
    PARSE_tkemodel,
    PARSE_deformations,
    PARSE_kcksyst,
    PARSE_yamodel
)
    
PARSER_MAP = {
    'gstrength_gdr': PARSE_gstrength,
    'spinscaling': PARSE_spinscaling,
    'rta': PARSE_rta,
    'tkemodel': PARSE_tkemodel,
    'deformations': PARSE_deformations,
    'kcksyst': PARSE_kcksyst,
    'yamodel': PARSE_yamodel
}

def identify_dat_file_type(filepath: Path) -> str:
    """
    Helper to detect file type from filename.
    
    Maps filename patterns to parser keys:
    - gstrength_gdr_params.dat → 'gstrength_gdr'
    - spinscalingmodel.dat → 'spinscaling'
    - rta.dat → 'rta'
    - tkemodel.dat → 'tkemodel'
    - deformations.dat → 'deformations'
    - kcksyst.dat → 'kcksyst'
    - yamodel.dat → 'yamodel'
    """
    
    filename = filepath.name.lower()
    
    # Check each parser key - order matters for specificity
    for key in PARSER_MAP.keys():
        if key in filename:
            return key
    
    return 'unknown'

def parse_dat_file(filepath: Path, preserve_format: bool = True, **kwargs) -> Tuple[Dict[str, Any], Optional[Dict]]:
    
    """
    Dispatcher: Identifies file type and calls the specific parse() function.
    
    Accepts extra arguments (like target_zaid) via **kwargs and passes them along.
    
    Args:
        filepath: Path to the .dat file
        preserve_format: If True, return format_info for exact reconstruction
        **kwargs: Additional arguments passed to specific parsers
                 Common: target_zaid (required for yamodel, kcksyst, deformations)
    
    Returns:
        Tuple of (params_dict, format_info_dict)
        - params_dict: Parameter values (structure varies by file type)
        - format_info_dict: Format preservation metadata (None if preserve_format=False)
        """
        
    file_type = identify_dat_file_type(filepath)
    
    if file_type == 'unknown':
        raise ValueError(f"Unknown .dat file type: {filepath.name}")
        
    if file_type not in PARSER_MAP:
        raise NotImplementedError(f"Parser for {file_type} is not linked in PARSER_MAP")
    
    # Get the module
    module = PARSER_MAP[file_type]
    
    # **kwargs passes 'target_zaid' down to the module automatically
    return module.parse(filepath, preserve_format=preserve_format, **kwargs)

def write_dat_file(filepath: Path, data: Dict[str, Any], format_info: Optional[Dict] = None, **kwargs) -> None:
    
    """
    Dispatcher: Identifies file type and calls the specific write() function.
    
    Args:
        filepath: Output file path
        data: Parameter data (structure varies by file type)
        format_info: Format preservation metadata from parse()
        **kwargs: Additional arguments passed to specific writers
                 Common: scale_factors, target_zaid, alpha_0_scale, etc.
    """
    
    file_type = identify_dat_file_type(filepath)
    
    if file_type == 'unknown':
        raise ValueError(f"Unknown .dat file type: {filepath.name}")
    
    module = PARSER_MAP[file_type]
    
    # **kwargs passes 'scale_factors', 'target_zaid', etc. down automatically
    module.write(filepath, data, format_info=format_info, **kwargs)
