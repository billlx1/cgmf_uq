#!/usr/bin/env python3
"""
Dry-run test for dat_parser.

Creates side-by-side ORIGINAL_*.dat and processed *.dat files for inspection.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cgmf_uq.io.dat_parser import parse_dat_file, write_dat_file

TARGET_FILES = [
    "gstrength_gdr_params.dat",
    "spinscalingmodel.dat",
    "rta.dat",
    "tkemodel.dat",
]

DEFAULT_SOURCE_DIR = PROJECT_ROOT / "CGMF_Data_Default"
DEFAULT_RESULTS_DIR = PROJECT_ROOT / "DryTest_Results"
DEFAULT_SCALES_JSON = Path(__file__).with_name("test_scale_factors.json")


def _load_scale_factors(path: Path) -> Dict[str, Any]:
    with path.open("r") as handle:
        return json.load(handle)


def _get_file_type(filename: str) -> str:
    filename_lower = filename.lower()
    if "gstrength_gdr" in filename_lower:
        return "gstrength_gdr"
    if "spinscaling" in filename_lower:
        return "spinscaling"
    if "rta" in filename_lower:
        return "rta"
    if "tkemodel" in filename_lower:
        return "tkemodel"
    raise ValueError(f"Unknown file type: {filename}")


def _scale_kwargs(file_type: str, scale_factors: Dict[str, Any], target_zaid: int) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"target_zaid": target_zaid}
    if file_type == "gstrength_gdr":
        kwargs["scale_factors"] = scale_factors["gstrength_gdr"]
    elif file_type == "spinscaling":
        kwargs["alpha_0_scale"] = scale_factors["spinscaling"]["alpha_0_scale"]
        kwargs["alpha_slope_scale"] = scale_factors["spinscaling"]["alpha_slope_scale"]
    elif file_type == "rta":
        kwargs["scale_factor"] = scale_factors["rta"]["scale_factor"]
    elif file_type == "tkemodel":
        kwargs["tke_en_scales"] = scale_factors["tkemodel"]["tke_en_scales"]
        kwargs["tke_ah_scales"] = scale_factors["tkemodel"]["tke_ah_scales"]
        kwargs["sigma_tke_scales"] = scale_factors["tkemodel"]["sigma_tke_scales"]
    return kwargs


def run(output_dir: Path, target_zaid: int, source_dir: Path, scales_json: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    scale_factors = _load_scale_factors(scales_json)

    print(f"Project root: {PROJECT_ROOT}")
    print(f"Source dir:   {source_dir}")
    print(f"Output dir:   {output_dir}")
    print(f"Scales JSON:  {scales_json}")

    for filename in TARGET_FILES:
        source_file = source_dir / filename
        if not source_file.exists():
            print(f"WARNING: Missing {source_file} (skipping)")
            continue

        original_copy = output_dir / f"ORIGINAL_{filename}"
        processed_file = output_dir / filename

        shutil.copy2(source_file, original_copy)

        params, format_info = parse_dat_file(
            source_file,
            preserve_format=True,
            target_zaid=target_zaid,
        )

        file_type = _get_file_type(filename)
        kwargs = _scale_kwargs(file_type, scale_factors, target_zaid)
        write_dat_file(processed_file, params, format_info, **kwargs)

        print(f"Wrote {processed_file.name} with ORIGINAL_{filename}")

    print("\nDone. Inspect side-by-side files in the output directory.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run test for dat_parser")
    parser.add_argument("--target-zaid", type=int, default=92235, help="Target ZAID")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR, help="Source .dat directory")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR, help="Dry test results root")
    parser.add_argument("--scales-json", type=Path, default=DEFAULT_SCALES_JSON, help="Scale factors JSON")
    parser.add_argument("--clean", action="store_true", help="Remove DryTest_Results and exit")

    args = parser.parse_args()

    if args.clean:
        if args.results_dir.exists():
            shutil.rmtree(args.results_dir)
            print(f"Removed {args.results_dir}")
        else:
            print(f"No results directory to remove: {args.results_dir}")
        return

    run(
        output_dir=args.results_dir / "dat_parser",
        target_zaid=args.target_zaid,
        source_dir=args.source_dir,
        scales_json=args.scales_json,
    )


if __name__ == "__main__":
    main()
