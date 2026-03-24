#!/usr/bin/env python3
"""
Dry-run generator for sampling JSONs (no SLURM submission).
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cgmf_uq.io.generate_sampling_json import generate_sampling


def parse_args():
    parser = argparse.ArgumentParser(
        description="Dry-run: generate sampling JSONs and manifest only",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--registry", required=True, help="Path to Parameter_Registry.yaml")
    parser.add_argument("--sampling", required=True, help="Path to Sampling_Config.yaml")
    parser.add_argument("--output", required=True, help="Output directory for JSONs/manifest")
    parser.add_argument("--cgmf-default-data", required=True, help="Path to CGMF_Data_Default")
    parser.add_argument("--target-id", type=int, default=92235, help="Target ZAID")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output directory")
    return parser.parse_args()


def main():
    args = parse_args()
    class Args:
        registry = Path(args.registry)
        sampling = Path(args.sampling)
        output = Path(args.output)
        cgmf_default_data = Path(args.cgmf_default_data)
        target_id = args.target_id
        force = args.force

    generate_sampling(Args)


if __name__ == "__main__":
    main()
