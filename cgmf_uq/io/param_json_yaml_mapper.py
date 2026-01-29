# cgmf_uq/workflow/param_mapper.py

"""
Parameter Mapper
================

Core translation layer between Parameter_Registry.yaml nomenclature and the JSON
structure expected by dat_generator.py.

Responsibilities:
- Load and parse Parameter_Registry.yaml
- Build reverse lookup tables (registry param name → JSON location)
- Translate registry-style parameter dictionaries to complete JSON structures
- Validate that all parameters are recognized

Design Decisions:
- Hardcoded DEFAULT_JSON_TEMPLATE: Template is tightly coupled to dat_generator.py
  and should be version-controlled with the code. This avoids external file dependencies
  and fails fast if the template is incomplete.
  
- Deep copy on every translation: Prevents accidental mutation of the class-level
  template, ensuring each JSON generation is independent.
  
- Runtime validation: Unknown parameters raise ValueError immediately, catching
  typos in config files before generating hundreds of JSON files.
  
- Support for heterogeneous scale types: Handles both scalar parameters and
  array element parameters through conditional dispatch based on scale_type.

Usage:
    mapper = ParameterMapper(Path("config/parameters.yaml"))
    
    # Generate JSON with single perturbation
    json_data = mapper.registry_to_json_structure({'global_PSF_norm': 1.1})
    
    # Generate baseline (all 1.0)
    baseline = mapper.registry_to_json_structure({})
"""

from typing import Dict, Any, Optional
from pathlib import Path
import yaml
import copy

import sys
# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))


class ParameterMapper:
    """
    Translates between Parameter_Registry.yaml nomenclature and JSON structure.
    
    This class serves as the single source of truth for how registry parameter
    names map to the JSON structure consumed by dat_generator.py. It ensures
    that sensitivity.py and sampling.py use consistent naming without duplicating
    the mapping logic.
    """
    
    # =========================================================================
    # Default JSON Template
    # =========================================================================
    # This is the complete structure expected by dat_generator.py with all
    # scale factors set to 1.0 (no perturbation). It must contain all 55
    # parameters even when only perturbing a single parameter.
    #
    # Motivation for hardcoding:
    # - Template structure is tightly coupled to dat_generator.py implementation
    # - Version controlled alongside the code that uses it
    # - No external file dependency = one less failure point
    # - Fast (no I/O required)
    #
    # Maintenance note: If you add parameters to the registry or change the
    # JSON structure expected by dat_generator.py, update this template.
    # =========================================================================
    
    DEFAULT_JSON_TEMPLATE = {
        "spinscaling": {
            "alpha_0_scale": 1.0,
            "alpha_slope_scale": 1.0
        },
        "rta": {
            "scale_factor": 1.0
        },
        "tkemodel": {
            "tke_en_scales": [1.0, 1.0, 1.0, 1.0],
            "tke_ah_scales": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "sigma_tke_scales": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        },
        "gstrength_gdr": {
            "global_PSF_norm": 1.0,
            "E1_DArigo_E_const1": 1.0,
            "E1_DArigo_E_const2": 1.0,
            "E1_DArigo_E_exp": 1.0,
            "E1_DArigo_W_factor": 1.0,
            "E1_DArigo_S_coef": 1.0,
            "E1_DH0_E_const": 1.0,
            "E1_DH0_E_exp_mass": 1.0,
            "E1_DH0_E_exp_beta": 1.0,
            "E1_DH0_W_const": 1.0,
            "E1_DH0_W_beta_coef": 1.0,
            "E1_DH0_S_coef": 1.0,
            "E1_DH1_E_const": 1.0,
            "E1_DH1_E_exp_mass": 1.0,
            "E1_DH1_W_const": 1.0,
            "E1_DH1_W_beta_coef": 1.0,
            "E1_DH1_S_coef": 1.0,
            "M1_E_const": 1.0,
            "M1_E_exp": 1.0,
            "M1_W_val": 1.0,
            "M1_S_val": 1.0,
            "E2_E_const": 1.0,
            "E2_E_exp": 1.0,
            "E2_W_const": 1.0,
            "E2_W_mass_coef": 1.0,
            "E2_S_coef": 1.0
        }
    }
    
    def __init__(self, registry_path: Path):
        """
        Initialize the parameter mapper by loading the registry and building
        lookup tables.
        
        Args:
            registry_path: Path to Parameter_Registry.yaml
            
        Raises:
            FileNotFoundError: If registry file doesn't exist
            yaml.YAMLError: If registry file is malformed
        """
        if not registry_path.exists():
            raise FileNotFoundError(f"Registry not found: {registry_path}")
        
        with open(registry_path, 'r') as f:
            self.registry = yaml.safe_load(f)
        
        # Build the reverse lookup table: registry_param_name → JSON location
        self._build_lookup_tables()
        
        # Validate that template matches registry (catches missing parameters)
        self._validate_template()
    
    def _build_lookup_tables(self):
        """
        Parse the registry and build reverse lookup tables.
        
        Creates self.param_to_json: A dictionary mapping each registry parameter
        name to its location and scaling strategy in the JSON structure.
        
        Example mapping:
            'global_PSF_norm' → {
                'json_section': 'gstrength_gdr',
                'json_key': 'global_PSF_norm',
                'scale_type': 'scalar',
                'default': 1.0
            }
            
            'Stke_en_c0' → {
                'json_section': 'tkemodel',
                'json_key': 'tke_en_scales',
                'array_index': 0,
                'scale_type': 'array_element',
                'default': 1.0
            }
        
        Design rationale:
        - Centralizes the registry → JSON mapping in one place
        - Supports both scalar parameters (scale_parameter) and array elements
          (scale_array_name + scale_array_index)
        - Enables runtime validation of parameter names
        """
        self.param_to_json = {}
        
        # Iterate through each .dat file group in the registry
        for dat_group_name, dat_group in self.registry.items():
            # Skip non-parameter entries (e.g., metadata, comments)
            if not isinstance(dat_group, dict) or 'parameters' not in dat_group:
                continue
            
            # Determine JSON section name by stripping '_params' suffix
            # Example: 'gstrength_gdr_params' → 'gstrength_gdr'
            json_section = dat_group_name.replace('_params', '')
            
            # Process each parameter in this group
            for param_name, param_info in dat_group['parameters'].items():
                
                # Case 1: Scalar parameter (e.g., global_PSF_norm)
                # These map directly to a JSON key
                if 'scale_parameter' in param_info:
                    self.param_to_json[param_name] = {
                        'json_section': json_section,
                        'json_key': param_info['scale_parameter'],
                        'scale_type': 'scalar',
                        'default': param_info.get('default', 1.0)
                    }
                
                # Case 2: Array element parameter (e.g., Stke_en_c0)
                # These map to a specific index in a JSON array
                elif 'scale_array_name' in param_info:
                    self.param_to_json[param_name] = {
                        'json_section': json_section,
                        'json_key': param_info['scale_array_name'],
                        'array_index': param_info['scale_array_index'],
                        'scale_type': 'array_element',
                        'default': param_info.get('default', 1.0)
                    }
    
    def _validate_template(self):
        """
        Cross-check that DEFAULT_JSON_TEMPLATE contains all parameters from registry.
        
        This catches configuration errors early:
        - Missing sections in template
        - Missing scalar keys
        - Arrays that are too short for their indices
        
        Raises:
            ValueError: If template is incomplete or inconsistent with registry
            
        Design rationale:
        - Fail fast: Better to crash on initialization than generate 300 bad JSON files
        - Self-documenting: Error messages explain exactly what's missing
        - Defensive: Protects against registry updates that don't update the template
        """
        for param_name, mapping in self.param_to_json.items():
            section = mapping['json_section']
            
            # Check section exists
            if section not in self.DEFAULT_JSON_TEMPLATE:
                raise ValueError(
                    f"Template validation failed: Missing section '{section}' "
                    f"required by parameter '{param_name}'"
                )
            
            if mapping['scale_type'] == 'scalar':
                # Check scalar key exists
                key = mapping['json_key']
                if key not in self.DEFAULT_JSON_TEMPLATE[section]:
                    raise ValueError(
                        f"Template validation failed: Missing key '{key}' in "
                        f"section '{section}' (required by '{param_name}')"
                    )
            
            elif mapping['scale_type'] == 'array_element':
                # Check array exists and is long enough
                array_name = mapping['json_key']
                index = mapping['array_index']
                
                if array_name not in self.DEFAULT_JSON_TEMPLATE[section]:
                    raise ValueError(
                        f"Template validation failed: Missing array '{array_name}' "
                        f"in section '{section}' (required by '{param_name}')"
                    )
                
                array_length = len(self.DEFAULT_JSON_TEMPLATE[section][array_name])
                if array_length <= index:
                    raise ValueError(
                        f"Template validation failed: Array '{array_name}' in "
                        f"section '{section}' has length {array_length}, but "
                        f"parameter '{param_name}' requires index {index}"
                    )
    
    def registry_to_json_structure(self, param_scales: Dict[str, float]) -> Dict[str, Any]:
        """
        Convert registry-style parameter dictionary to complete JSON structure.
        
        This is the core translation method. It takes a dictionary of parameter
        perturbations (using registry names) and produces a complete 55-parameter
        JSON file suitable for dat_generator.py.
        
        Args:
            param_scales: Dictionary of {registry_param_name: scale_factor}
                Examples:
                - {} → All parameters at 1.0 (baseline)
                - {'global_PSF_norm': 1.1} → Only global_PSF_norm perturbed
                - {'global_PSF_norm': 1.1, 'Stke_en_c0': 0.95} → Two perturbations
        
        Returns:
            Complete JSON structure with all 55 parameters. Only the parameters
            specified in param_scales will differ from 1.0.
            
        Raises:
            ValueError: If any parameter name in param_scales is not recognized
            
        Design rationale:
        - Deep copy template: Prevents accidental mutation of class-level constant
        - Validate all parameters: Catches typos before generating files
        - Complete output: dat_generator.py requires all 55 parameters, even if
          only perturbing one
          
        Example:
            >>> mapper = ParameterMapper(Path("config/parameters.yaml"))
            >>> json_data = mapper.registry_to_json_structure({'global_PSF_norm': 1.1})
            >>> json_data['gstrength_gdr']['global_PSF_norm']
            1.1
            >>> json_data['gstrength_gdr']['E1_DArigo_E_const1']
            1.0
        """
        # Start with a deep copy to avoid mutating the class-level template
        # This ensures each call is independent and thread-safe
        json_structure = copy.deepcopy(self.DEFAULT_JSON_TEMPLATE)
        
        # Apply each perturbation
        for reg_param_name, scale_value in param_scales.items():
            
            # Validate parameter name (catch typos early)
            if reg_param_name not in self.param_to_json:
                known_params = sorted(self.param_to_json.keys())
                raise ValueError(
                    f"Unknown parameter '{reg_param_name}'. This parameter is not "
                    f"defined in Parameter_Registry.yaml. Known parameters:\n" +
                    "\n".join(f"  - {p}" for p in known_params[:10]) +
                    f"\n  ... and {len(known_params) - 10} more"
                    if len(known_params) > 10 else ""
                )
            
            # Retrieve mapping information
            mapping = self.param_to_json[reg_param_name]
            section = mapping['json_section']
            
            # Case 1: Scalar parameter
            # Directly assign the scale value to the JSON key
            if mapping['scale_type'] == 'scalar':
                json_structure[section][mapping['json_key']] = scale_value
            
            # Case 2: Array element parameter
            # Update a specific index in the JSON array
            elif mapping['scale_type'] == 'array_element':
                array_name = mapping['json_key']
                index = mapping['array_index']
                json_structure[section][array_name][index] = scale_value
        
        return json_structure
    
    def get_parameter_info(self, param_name: str) -> Dict[str, Any]:
        """
        Retrieve metadata about a parameter from the registry.
        
        Useful for generating documentation or validation reports.
        
        Args:
            param_name: Registry parameter name
            
        Returns:
            Dictionary containing parameter metadata (id, units, description, etc.)
            
        Raises:
            ValueError: If parameter name is not recognized
        """
        if param_name not in self.param_to_json:
            raise ValueError(f"Unknown parameter: {param_name}")
        
        # Find the parameter in the registry
        for dat_group in self.registry.values():
            if isinstance(dat_group, dict) and 'parameters' in dat_group:
                if param_name in dat_group['parameters']:
                    return dat_group['parameters'][param_name]
        
        raise ValueError(f"Parameter {param_name} found in lookup but not in registry")
    
    def list_all_parameters(self) -> list:
        """
        Get a sorted list of all recognized parameter names.
        
        Useful for:
        - Validating sensitivity.yaml completeness
        - Generating documentation
        - Interactive exploration
        
        Returns:
            Sorted list of all registry parameter names
        """
        return sorted(self.param_to_json.keys())

