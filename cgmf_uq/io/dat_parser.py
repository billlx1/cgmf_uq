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

from cgmf_uq.io.FILE_PARSERS import PARSE_gstrength, PARSE_spinscaling, PARSE_rta, PARSE_tkemodel

    
PARSER_MAP = {
    'gstrength_gdr': PARSE_gstrength,
    'spinscaling': PARSE_spinscaling,
    'rta': PARSE_rta,
    'tkemodel': PARSE_tkemodel
}

def identify_dat_file_type(filepath: Path) -> str:
    """Helper to detect file type from filename."""
    filename = filepath.name.lower()
    for key in PARSER_MAP.keys():
        if key in filename:
            return key
    return 'unknown'

def parse_dat_file(filepath: Path, preserve_format: bool = True, **kwargs) -> Tuple[Dict[str, Any], Optional[Dict]]:
    """
    Dispatcher: Identifies file type and calls the specific parse() function.
     Accepts extra arguments (like target_zaid) via **kwargs and passes them along.
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
    """
    file_type = identify_dat_file_type(filepath)
    
    if file_type == 'unknown':
        raise ValueError(f"Unknown .dat file type: {filepath.name}")

    module = PARSER_MAP[file_type]
    
    # **kwargs passes 'scale_factors', 'alpha_0_scale', etc. down automatically
    module.write(filepath, data, format_info=format_info, **kwargs)
