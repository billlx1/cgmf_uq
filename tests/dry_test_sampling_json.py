#!/usr/bin/env python3
"""
Dry-run tests for sampling JSON generation.
"""
from __future__ import annotations

import io
import sys
import tempfile
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cgmf_uq.io.generate_sampling_json import generate_sampling


def _write_yaml(path: Path, text: str) -> None:
    path.write_text(text)


def run() -> None:
    registry_path = PROJECT_ROOT / "config" / "Parameter_Registry.yaml"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        sampling_cfg = tmp_dir / "Sampling_Config.yaml"
        output_dir = tmp_dir / "out"

        _write_yaml(
            sampling_cfg,
            f"""
num_samples: 3
seed: 123
sampling_info_dir: {PROJECT_ROOT / "tests"}

groups:
  - name: tke_en_correlated
    enabled: true
    parameters: [tke_en_a, tke_en_e0, tke_en_b, tke_en_d]
    sampler: mvn_cholesky
    value_space: absolute
    params:
      json_file: TKE_en_Sample.json

  - name: tke_ah_correlated
    enabled: true
    parameters: [tke_ah_a0, tke_ah_amax, tke_ah_c0, tke_ah_c1, tke_ah_c2, tke_ah_c3, tke_ah_c4, tke_ah_c5, tke_ah_c6, tke_ah_c7, tke_ah_c8]
    sampler: mvn_cholesky
    value_space: absolute
    params:
      json_file: TKE_Ah_Sample.json

  - name: stke_ah_correlated
    enabled: true
    parameters: [Stke_ah_a0, Stke_ah_amax, Stke_ah_c0, Stke_ah_c1, Stke_ah_c2, Stke_ah_c3, Stke_ah_c4, Stke_ah_c5, Stke_ah_c6, Stke_ah_c7, Stke_ah_c8]
    sampler: mvn_cholesky
    value_space: absolute
    params:
      json_file: STKE_Ah_Sample.json
""",
        )

        class Args:
            registry = registry_path
            sampling = sampling_cfg
            output = output_dir
            force = True
            cgmf_default_data = PROJECT_ROOT / "CGMF_Data_Default"
            target_id = 92235

        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            generate_sampling(Args)

        json_path = output_dir / "configs" / "sample_00000.json"
        if not json_path.exists():
            raise AssertionError("Expected JSON output not found")

        # Basic sanity check: ensure TKE scales are finite
        import json as _json
        data = _json.loads(json_path.read_text())
        tke_en = data["tkemodel"]["tke_en_scales"]
        if not all(np.isfinite(tke_en)):
            raise AssertionError("Non-finite values in tke_en_scales")

        # Validation: duplicate parameter in two groups
        bad_cfg = tmp_dir / "Sampling_Bad.yaml"
        _write_yaml(
            bad_cfg,
            """
num_samples: 1
seed: 1

groups:
  - name: g1
    parameters: [global_PSF_norm]
    sampler: independent_gaussian
    value_space: scale
    params:
      stddev: 0.1
  - name: g2
    parameters: [global_PSF_norm]
    sampler: independent_gaussian
    value_space: scale
    params:
      stddev: 0.1
""",
        )

        class BadArgs:
            registry = registry_path
            sampling = bad_cfg
            output = output_dir / "bad"
            force = True
            cgmf_default_data = PROJECT_ROOT / "CGMF_Data_Default"
            target_id = 92235

        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                generate_sampling(BadArgs)
        except Exception as exc:
            if "appears in multiple groups" not in str(exc):
                raise AssertionError("Expected duplicate parameter error") from exc
        else:
            raise AssertionError("Expected error for duplicate parameter")

        # Validation: missing mvn files
        bad_cfg2 = tmp_dir / "Sampling_Bad_MVN.yaml"
        _write_yaml(
            bad_cfg2,
            """
num_samples: 1
seed: 1

groups:
  - name: mvn_missing
    parameters: [MY_AS1_Wa]
    sampler: mvn_cholesky
    value_space: absolute
    params: {}
""",
        )

        class BadArgs2:
            registry = registry_path
            sampling = bad_cfg2
            output = output_dir / "bad2"
            force = True
            cgmf_default_data = PROJECT_ROOT / "CGMF_Data_Default"
            target_id = 92235

        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                generate_sampling(BadArgs2)
        except Exception as exc:
            if "mu_file" not in str(exc):
                raise AssertionError("Expected missing mvn file error") from exc
        else:
            raise AssertionError("Expected error for missing mvn files")

    print("Sampling JSON generation tests: OK")


if __name__ == "__main__":
    run()
