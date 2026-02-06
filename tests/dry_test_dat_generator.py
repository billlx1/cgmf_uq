#!/usr/bin/env python3
"""
Dry-run test for dat_generator.

Creates a full generated directory plus ORIGINAL_*.dat copies for side-by-side inspection.
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

from cgmf_uq.io.dat_generator import generate_perturbed_dat_files

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


def run(output_dir: Path, target_zaid: int, source_dir: Path, scales_json: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    scale_factors = _load_scale_factors(scales_json)

    print(f"Project root: {PROJECT_ROOT}")
    print(f"Source dir:   {source_dir}")
    print(f"Output dir:   {output_dir}")
    print(f"Scales JSON:  {scales_json}")

    generate_perturbed_dat_files(
        output_dir=output_dir,
        target_zaid=target_zaid,
        scale_factors=scale_factors,
        source_dir=source_dir,
        verbose=True,
    )

    for filename in TARGET_FILES:
        source_file = source_dir / filename
        if not source_file.exists():
            print(f"WARNING: Missing {source_file} (skipping ORIGINAL_ copy)")
            continue

        original_copy = output_dir / f"ORIGINAL_{filename}"
        shutil.copy2(source_file, original_copy)
        print(f"Added {original_copy.name} for side-by-side inspection")

    print("\nDone. Inspect side-by-side files in the output directory.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run test for dat_generator")
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
        output_dir=args.results_dir / "dat_generator",
        target_zaid=args.target_zaid,
        source_dir=args.source_dir,
        scales_json=args.scales_json,
    )


if __name__ == "__main__":
    main()
