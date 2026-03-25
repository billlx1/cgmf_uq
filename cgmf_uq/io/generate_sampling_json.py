#!/usr/bin/env python3
"""
Phase II: Sampling Input Generator

Generates scale-factor JSONs and a manifest for sampling runs.
"""
from __future__ import annotations

import sys
import yaml
import json
import argparse
from pathlib import Path
import shutil
from typing import Dict, Any, List

import numpy as np

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from cgmf_uq.io.param_json_yaml_mapper import ParameterMapper
from cgmf_uq.sampling.samplers import SamplerContext, build_sampler
from cgmf_uq.io.dat_parser import parse_dat_file


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate sampling input files for HPC runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("config/Parameter_Registry.yaml"),
        help="Path to Parameter Registry YAML (default: %(default)s)",
    )
    parser.add_argument(
        "--sampling",
        type=Path,
        default=Path("config/Sampling_Config.yaml"),
        help="Path to Sampling Config YAML (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/sampling_inputs"),
        help="Output directory for generated files (default: %(default)s)",
    )
    parser.add_argument(
        "--cgmf-default-data",
        type=Path,
        default=project_root / "CGMF_Data_Default",
        help="Path to default CGMF data directory (default: %(default)s)",
    )
    parser.add_argument(
        "--target-id",
        type=int,
        default=92235,
        help="Target ZAID used to read defaults from CGMF data",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output directory without prompting",
    )
    parser.add_argument(
        "--reuse-configs",
        type=Path,
        default=None,
        help="Path to prior output directory or configs/ dir to reuse scale factors",
    )
    parser.add_argument(
        "--reuse-groups",
        type=str,
        default=None,
        help="Comma-separated group names to reuse, or 'all', or 'all-except:grp1,grp2'",
    )
    return parser.parse_args()


def _validate_inputs(registry_path: Path, sampling_path: Path) -> None:
    if not registry_path.exists():
        raise FileNotFoundError(f"Registry not found: {registry_path}")
    if not sampling_path.exists():
        raise FileNotFoundError(f"Sampling config not found: {sampling_path}")


def _validate_groups(groups: List[Dict[str, Any]]) -> None:
    seen = set()
    for group in groups:
        params = _normalize_parameters(group.get("parameters", []))
        for p in params:
            if p in seen:
                raise ValueError(f"Parameter '{p}' appears in multiple groups")
            seen.add(p)


def _resolve_sampling_info_dir(config: Dict[str, Any], sampling_path: Path) -> Path:
    sampling_info = config.get("sampling_info_dir", "sampling_info")
    p = Path(sampling_info)
    if not p.is_absolute():
        p = sampling_path.parent / p
    return p


def _load_mvn_mu_fallback(json_path: Path) -> Dict[str, float]:
    text = json_path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"MVN JSON file is empty: {json_path}")
    data = json.loads(text)
    required = {"param_labels", "gauss_mu"}
    if not required.issubset(set(data.keys())):
        missing = required - set(data.keys())
        raise ValueError(f"MVN JSON missing keys for fallback: {sorted(missing)}")
    labels = list(data["param_labels"])
    mu = list(data["gauss_mu"])
    if len(labels) != len(mu):
        raise ValueError("MVN JSON param_labels and gauss_mu length mismatch")
    return {labels[i]: float(mu[i]) for i in range(len(labels))}


def _normalize_parameters(param_spec: Any) -> List[str]:
    if isinstance(param_spec, list):
        return list(param_spec)
    if isinstance(param_spec, dict):
        return [name for name, cfg in param_spec.items() if cfg is None or cfg.get("enabled", True)]
    raise ValueError("parameters must be a list or dict")


def _resolve_reuse_groups(reuse_spec: str, enabled_groups: Dict[str, Dict[str, Any]]) -> List[str]:
    if reuse_spec == "all":
        return list(enabled_groups.keys())
    if reuse_spec.startswith("all-except:"):
        raw = reuse_spec.split(":", 1)[1]
        exclude = {g.strip() for g in raw.split(",") if g.strip()}
        return [g for g in enabled_groups.keys() if g not in exclude]
    return [g.strip() for g in reuse_spec.split(",") if g.strip()]


def _load_reuse_samples(reuse_configs: Path) -> List[Dict[str, Any]]:
    reuse_configs = reuse_configs.resolve()
    manifest = reuse_configs / "manifest.csv" if reuse_configs.is_dir() else None
    configs_dir = reuse_configs / "configs" if reuse_configs.is_dir() else None

    if reuse_configs.is_file():
        raise ValueError("reuse-configs must be a directory containing configs/ or manifest.csv")

    json_paths: List[Path] = []
    if manifest and manifest.exists():
        lines = manifest.read_text().splitlines()
        header = lines[0].split(",") if lines else []
        if "config_file" not in header:
            raise ValueError("reuse manifest missing config_file column")
        idx = header.index("config_file")
        for line in lines[1:]:
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) <= idx:
                continue
            p = Path(parts[idx])
            if not p.is_absolute():
                p = manifest.parent / p
            json_paths.append(p)
    elif configs_dir and configs_dir.exists():
        json_paths = sorted(configs_dir.glob("*.json"))
    else:
        raise ValueError("reuse-configs must contain manifest.csv or configs/*.json")

    samples = []
    for p in json_paths:
        samples.append(json.loads(p.read_text()))
    return samples


def _extract_scale_factor(sample_json: Dict[str, Any], mapper: ParameterMapper, param_name: str) -> float:
    info = mapper.param_to_json[param_name]
    section = info["json_section"]
    if info["scale_type"] == "scalar":
        return float(sample_json[section][info["json_key"]])
    array_name = info["json_key"]
    idx = info["array_index"]
    return float(sample_json[section][array_name][idx])


def _apply_value_space(
    values: Dict[str, np.ndarray],
    value_space: str,
    dat_defaults: Dict[str, float],
    fallback_defaults: Dict[str, float] | None = None,
) -> Dict[str, np.ndarray]:
    if value_space not in {"scale", "absolute"}:
        raise ValueError("value_space must be 'scale' or 'absolute'")
    if value_space == "scale":
        return values
    out: Dict[str, np.ndarray] = {}
    for p, v in values.items():
        if p not in dat_defaults:
            raise ValueError(f"Missing default value for parameter '{p}' from .dat files")
        denom = dat_defaults[p]
        if denom == 0.0:
            if fallback_defaults and p in fallback_defaults and fallback_defaults[p] != 0.0:
                denom = fallback_defaults[p]
            else:
                print(
                    f"WARNING: Default value for '{p}' is zero and no nonzero fallback found; "
                    f"using 1.0 as denominator (scale factor equals absolute value)."
                )
                denom = 1.0
        out[p] = v / denom
    return out


def _load_dat_defaults(
    parameters: List[str],
    registry_path: Path,
    cgmf_default_data: Path,
    target_id: int,
) -> Dict[str, float]:
    registry = yaml.safe_load(registry_path.read_text())
    param_map: Dict[str, Dict[str, Any]] = {}
    section_files: Dict[str, str] = {}

    for dat_group_name, dat_group in registry.items():
        if not isinstance(dat_group, dict) or "parameters" not in dat_group:
            continue
        json_section = dat_group_name.replace("_params", "")
        dat_file = dat_group.get("dat_file")
        if dat_file:
            section_files[json_section] = dat_file
        for param_name, param_info in dat_group["parameters"].items():
            if "scale_parameter" in param_info:
                param_map[param_name] = {
                    "json_section": json_section,
                    "json_key": param_info["scale_parameter"],
                    "scale_type": "scalar",
                }
            elif "scale_array_name" in param_info:
                param_map[param_name] = {
                    "json_section": json_section,
                    "json_key": param_info["scale_array_name"],
                    "array_index": param_info["scale_array_index"],
                    "scale_type": "array_element",
                }

    for p in parameters:
        if p not in param_map:
            raise ValueError(f"Parameter '{p}' not found in registry")

    needed_sections = {param_map[p]["json_section"] for p in parameters}
    parsed_by_section: Dict[str, Dict[str, Any]] = {}
    for section in needed_sections:
        if section not in section_files:
            raise ValueError(f"Missing dat_file mapping for section '{section}'")
        dat_path = cgmf_default_data / section_files[section]
        params, _ = parse_dat_file(dat_path, preserve_format=False, target_zaid=target_id)
        parsed_by_section[section] = params

    defaults: Dict[str, float] = {}
    for p in parameters:
        info = param_map[p]
        section = info["json_section"]
        parsed = parsed_by_section[section]

        if info["scale_type"] == "scalar":
            json_key = info["json_key"]
            if section == "rta" and json_key == "scale_factor":
                defaults[p] = 1.0
                continue
            candidates = [json_key]
            if json_key.endswith("_scale"):
                candidates.append(json_key.replace("_scale", ""))
            candidates.append(p)
            found = False
            for key in candidates:
                if key in parsed:
                    defaults[p] = float(parsed[key])
                    found = True
                    break
            if not found:
                raise ValueError(f"Default value for '{p}' not found in {section} parser output")

        elif info["scale_type"] == "array_element":
            array_key = info["json_key"]
            if array_key.endswith("_scales"):
                array_key = array_key.replace("_scales", "")
            if array_key not in parsed:
                raise ValueError(f"Array '{array_key}' not found in {section} parser output")
            arr = parsed[array_key]
            defaults[p] = float(arr[info["array_index"]])

    return defaults


def generate_sampling(args) -> None:
    registry_path = args.registry
    sampling_path = args.sampling
    output_dir = args.output
    cgmf_default_data = args.cgmf_default_data
    target_id = args.target_id
    reuse_configs = args.reuse_configs
    reuse_groups_spec = args.reuse_groups

    _validate_inputs(registry_path, sampling_path)

    if output_dir.exists():
        if not args.force:
            raise FileExistsError(f"Output directory exists: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    config_dir = output_dir / "configs"
    config_dir.mkdir(parents=True)

    with open(sampling_path, "r") as f:
        sampling_config = yaml.safe_load(f)

    num_samples = int(sampling_config.get("num_samples", 0))
    seed = sampling_config.get("seed", None)
    groups = sampling_config.get("groups", [])

    if num_samples <= 0:
        raise ValueError("num_samples must be > 0")
    if not groups:
        raise ValueError("No groups defined in sampling config")

    _validate_groups(groups)

    rng = np.random.default_rng(seed)
    mapper = ParameterMapper(registry_path)

    sampling_info_dir = _resolve_sampling_info_dir(sampling_config, sampling_path)

    # Build enabled group map
    enabled_groups: Dict[str, Dict[str, Any]] = {}
    for group in groups:
        if group.get("enabled", True):
            name = group.get("name", "")
            if not name:
                raise ValueError("Each group must have a name")
            enabled_groups[name] = group

    # Determine which parameters need defaults from .dat files
    absolute_params: List[str] = []
    for group in enabled_groups.values():
        if group.get("value_space", "scale") == "absolute":
            absolute_params.extend(_normalize_parameters(group.get("parameters", [])))

    dat_defaults = _load_dat_defaults(absolute_params, registry_path, cgmf_default_data, target_id)

    # Resolve reuse configuration
    reuse_params: set[str] = set()
    reuse_samples: List[Dict[str, Any]] = []
    if reuse_groups_spec:
        if not reuse_configs:
            raise ValueError("--reuse-groups requires --reuse-configs")
        reuse_group_names = _resolve_reuse_groups(reuse_groups_spec, enabled_groups)
        for g in reuse_group_names:
            if g not in enabled_groups:
                raise ValueError(f"Reuse group '{g}' not found among enabled groups")
            reuse_params.update(_normalize_parameters(enabled_groups[g].get("parameters", [])))
        reuse_samples = _load_reuse_samples(Path(reuse_configs))
        if len(reuse_samples) != num_samples:
            raise ValueError("reuse-configs sample count does not match num_samples")
    elif reuse_configs:
        raise ValueError("--reuse-configs requires --reuse-groups")

    # Build per-group samples
    group_samples: List[Dict[str, np.ndarray]] = []
    for group in enabled_groups.values():
        name = group.get("name", "")
        parameters = _normalize_parameters(group.get("parameters", []))
        sampler_name = group.get("sampler", "")
        value_space = group.get("value_space", "scale")
        params = group.get("params", {})
        fallback_defaults = None

        if not name:
            raise ValueError("Each group must have a name")
        if not parameters:
            raise ValueError(f"Group '{name}' has no parameters")
        if not sampler_name:
            raise ValueError(f"Group '{name}' missing sampler")

        # Resolve sampling_info_dir relative paths for known fields
        if "mu_file" in params:
            p = Path(params["mu_file"])
            params["mu_file"] = str(p if p.is_absolute() else sampling_info_dir / p)
        if "chol_file" in params:
            p = Path(params["chol_file"])
            params["chol_file"] = str(p if p.is_absolute() else sampling_info_dir / p)
        if "json_file" in params:
            p = Path(params["json_file"])
            params["json_file"] = str(p if p.is_absolute() else sampling_info_dir / p)
            if sampler_name == "mvn_cholesky" and value_space == "absolute":
                fallback_defaults = _load_mvn_mu_fallback(Path(params["json_file"]))
        if "custom_module" in params:
            p = Path(params["custom_module"])
            params["custom_module"] = str(p if p.is_absolute() else sampling_info_dir / p)

        # If independent_gaussian and no mean provided, set mean based on value_space
        if sampler_name == "independent_gaussian" and "mean" not in params:
            if value_space == "scale":
                params["mean"] = 1.0
            else:
                params["mean"] = {p: mapper.get_parameter_default(p) for p in parameters}

        ctx = SamplerContext(parameters=parameters, value_space=value_space, params=params)
        sampler = build_sampler(sampler_name, ctx)
        values = sampler.sample(num_samples, rng)
        if set(values.keys()) != set(parameters):
            raise ValueError(f"Sampler '{name}' returned mismatched parameters")
        for p in parameters:
            if len(values[p]) != num_samples:
                raise ValueError(f"Sampler '{name}' produced wrong length for {p}")
        values = _apply_value_space(values, value_space, dat_defaults, fallback_defaults)
        group_samples.append(values)

    # Merge group samples into per-sample scale factors
    all_samples: List[Dict[str, float]] = []
    for i in range(num_samples):
        sample: Dict[str, float] = {}
        for g in group_samples:
            for p, arr in g.items():
                sample[p] = float(arr[i])
        if reuse_params:
            old = reuse_samples[i]
            for p in reuse_params:
                sample[p] = _extract_scale_factor(old, mapper, p)
        all_samples.append(sample)

    # Write JSONs and manifest
    manifest_path = output_dir / "manifest.csv"
    manifest_lines = ["task_id,sample_id,config_file"]

    for task_id, sample in enumerate(all_samples):
        filename = f"sample_{task_id:05d}.json"
        file_path = config_dir / filename
        json_content = mapper.registry_to_json_structure(sample)
        with open(file_path, "w") as f:
            json.dump(json_content, f, indent=2)
        manifest_lines.append(f"{task_id},{task_id},{file_path.resolve()}")

    with open(manifest_path, "w") as f:
        f.write("\n".join(manifest_lines))

    print("Sampling JSON generation complete")
    print(f"  Output:   {output_dir.resolve()}")
    print(f"  Manifest: {manifest_path.resolve()}")
    print(f"  Samples:  {num_samples}")


if __name__ == "__main__":
    args = parse_args()
    generate_sampling(args)
